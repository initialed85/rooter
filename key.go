package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// sessionHeaderNames are probed in order. Go canonicalises header keys, so
// both dash and underscore spellings are needed ("session_id" -> "Session_id").
var sessionHeaderNames = []string{
	"X-Session-Id",
	"X-Session-Affinity",
	"X-Pi-Session-Id",
	"X-Conversation-Id",
	"X-Thread-Id",
	"Session-Id",
	"Session_id",
	"X-Openrouter-Session-Id",
	"X-Client-Session-Id",
}

// sessionBodyFields are probed in order out of the top level of a JSON body.
var sessionBodyFields = []string{
	"prompt_cache_key",
	"session_id",
	"cache_key",
	"user",
	"metadata.user_id",
	"id",
}

// requestInfo is what rooter needs to know about a request body. It is derived
// from a *prefix* of the body so that huge requests cost no more than
// -scan-window bytes to classify.
type requestInfo struct {
	Key       string
	KeySource string // header | field | prefix | none
	Model     string
}

// extractKey works out which conversation a request belongs to and which model
// it asks for.
//
//  1. an explicit session header (Pi can be configured to send these)
//  2. a session-ish field in the JSON body (prompt_cache_key, user, ...)
//  3. a hash of the conversation prefix - the system prompt plus the first user
//     turn. That is stable for the life of a session and different between
//     sessions, so sticky routing works with clients that know nothing about
//     rooter.
//
// The model is always taken from the body when there is one, because routing
// needs it even when the session came from a header.
func extractKey(h http.Header, body []byte, contentType string) requestInfo {
	info := requestInfo{KeySource: "none"}
	if len(body) > 0 && isJSONContent(contentType) {
		if got, ok := scanJSON(body); ok {
			info = got
		} else {
			info.Key = "p:" + prefixHash(body, nil)
			info.KeySource = "prefix"
		}
	}
	for _, name := range sessionHeaderNames {
		if v := strings.TrimSpace(h.Get(name)); v != "" {
			info.Key, info.KeySource = "h:"+v, "header"
			return info
		}
	}
	if info.Key == "" && len(body) > 0 {
		info.Key, info.KeySource = "p:"+prefixHash(body, nil), "prefix"
	}
	return info
}

func isJSONContent(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(ct))
	return ct == "" || strings.Contains(ct, "json")
}

// scanJSON walks the top level of an OpenAI-ish request body, pulling out the
// fields rooter cares about and the conversation prefix used for hashing. It
// stops early on malformed or truncated input, which is fine: whatever it saw
// is still a stable prefix.
func scanJSON(body []byte) (requestInfo, bool) {
	var (
		info    requestInfo
		scalars = map[string]string{}
		systems []string
		first   string
		ok      = true
	)
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return requestInfo{}, false
	}
	if d, isDelim := tok.(json.Delim); !isDelim || d != '{' {
		return requestInfo{}, false
	}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			ok = false
			break
		}
		key, isStr := kt.(string)
		if !isStr {
			ok = false
			break
		}
		switch key {
		case "messages":
			if err := scanMessages(dec, &systems, &first); err != nil {
				ok = false
			}
		case "model":
			if v, err := decodeScalar(dec); err == nil {
				scalars["model"] = v
			} else {
				ok = false
			}
		case "metadata":
			var m map[string]json.RawMessage
			if err := dec.Decode(&m); err != nil {
				ok = false
				break
			}
			for _, k := range []string{"user_id", "session_id", "prompt_cache_key"} {
				if v, err := decodeScalarBytes(m[k]); err == nil {
					scalars["metadata."+k] = v
				}
			}
		default:
			if wanted(key) {
				if v, err := decodeScalar(dec); err == nil {
					scalars[key] = v
				} else {
					ok = false
				}
				continue
			}
			if err := skipValue(dec); err != nil {
				ok = false
			}
		}
		if !ok {
			break
		}
	}
	info.Model = strings.TrimSpace(scalars["model"])
	for _, f := range sessionBodyFields {
		if v := strings.TrimSpace(scalars[f]); v != "" {
			info.Key = "b:" + f + ":" + v
			info.KeySource = "field"
			return info, true
		}
	}
	if len(systems) > 0 || first != "" {
		seed := bytes.Buffer{}
		seed.WriteString(info.Model)
		for _, s := range systems {
			seed.WriteString("\x00sys:")
			seed.WriteString(s)
		}
		seed.WriteString("\x00first:")
		seed.WriteString(first)
		info.Key = "p:" + prefixHash(nil, seed.Bytes())
		info.KeySource = "prefix"
		return info, true
	}
	if !ok {
		return requestInfo{}, false
	}
	return requestInfo{KeySource: "none"}, true
}

func wanted(key string) bool {
	for _, f := range sessionBodyFields {
		if f == key {
			return true
		}
	}
	return false
}

// maxCapture bounds how much of any single message we keep for hashing. The
// prefix of a turn never changes, so a truncated capture is still stable.
const maxCapture = 48 << 10

func scanMessages(dec *json.Decoder, systems *[]string, first *string) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if d, isDelim := tok.(json.Delim); !isDelim || d != '[' {
		return nil
	}
	for dec.More() {
		var el struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
			Text    string          `json:"text"`
		}
		if err := dec.Decode(&el); err != nil {
			return err
		}
		role := strings.ToLower(el.Role)
		capture := el.Content
		if len(capture) == 0 {
			capture = json.RawMessage(el.Text)
		}
		if len(capture) > maxCapture {
			capture = capture[:maxCapture]
		}
		switch role {
		case "system", "developer":
			*systems = append(*systems, string(capture))
		case "user", "human":
			if *first == "" {
				*first = string(capture)
			}
		}
	}
	_, err = dec.Token() // consume ']'
	return err
}

func decodeScalar(dec *json.Decoder) (string, error) {
	raw, err := dec.Token()
	if err != nil {
		return "", err
	}
	switch v := raw.(type) {
	case string:
		return v, nil
	case json.Delim: // object or array: hash its serialisation
		buf := &bytes.Buffer{}
		if err := reencode(dec, v, buf); err != nil {
			return "", err
		}
		return buf.String(), nil
	default:
		b, _ := json.Marshal(v)
		return string(b), nil
	}
}

// skipValue consumes one JSON value, including any nested structure. The token
// for the value itself has not been read yet.
func skipValue(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if _, isDelim := t.(json.Delim); !isDelim {
		return nil
	} else if d := t.(json.Delim); d != '{' && d != '[' {
		return nil
	}
	depth := 1
	for depth > 0 {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		if d, isDelim := t.(json.Delim); isDelim {
			if d == '[' || d == '{' {
				depth++
			} else {
				depth--
			}
		}
	}
	return nil
}

// reencode reads the remainder of a compound value (whose opening delimiter has
// already been consumed) back into buf as JSON.
func reencode(dec *json.Decoder, open json.Delim, buf *bytes.Buffer) error {
	buf.WriteByte(byte(open))
	depth := 1
	for depth > 0 {
		t, err := dec.Token()
		if err != nil {
			return err
		}
		switch v := t.(type) {
		case json.Delim:
			if v == '[' || v == '{' {
				depth++
			} else {
				depth--
			}
			buf.WriteByte(byte(v))
		case string:
			b, _ := json.Marshal(v)
			buf.Write(b)
		default:
			b, _ := json.Marshal(v)
			buf.Write(b)
		}
		if dec.More() && depth > 0 {
			buf.WriteByte(',')
		}
	}
	return nil
}

func decodeScalarBytes(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", errNoValue
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", err
	}
	return s, nil
}

var errNoValue = errors.New("no value")

func prefixHash(body, seed []byte) string {
	h := sha256.New()
	if seed != nil {
		h.Write(seed)
	} else {
		n := len(body)
		if n > maxCapture {
			n = maxCapture
		}
		h.Write(body[:n])
		h.Write([]byte{0, 0, byte(n)})
	}
	return hex.EncodeToString(h.Sum(nil))[:24]
}
