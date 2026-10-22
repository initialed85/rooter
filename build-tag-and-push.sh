#!/usr/bin/env bash
#
# Build, tag and push rooter.
#
#   ./build-tag-and-push.sh                 # -> initialed85/rooter:latest (+ :<git-sha>)
#   ./build-tag-and-push.sh v1.2.3          # -> initialed85/rooter:v1.2.3 (+ :<git-sha>)
#   NO_PUSH=1 ./build-tag-and-push.sh       # local build only, for testing the image
#
# The binary is compiled here, not inside Docker, so what gets shipped is the
# exact artifact the tests were run against. rooter is pure Go with CGO off, so
# the cross-compile from any host yields a static binary and the runtime image
# needs nothing but tzdata + curl for the healthcheck.
#
# Set DOCKER_HOST to build against another daemon, e.g. from icarus against
# edward-desktop:  DOCKER_HOST=tcp://192.168.11.128:2375 ./build-tag-and-push.sh

set -e

# run from anywhere; the script always works on its own repo
pushd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1
trap 'popd >/dev/null 2>&1 || true' exit

if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker not found" >&2
    exit 1
fi

IMAGE_REPO="${IMAGE_REPO:-initialed85/rooter}"
TAG="${1:-latest}"
GOOS_TARGET="${GOOS_TARGET:-linux/amd64}"

mkdir -p ./tmp
echo "==> building rooter for ${GOOS_TARGET}"
# -trimpath keeps build paths out of the binary so the same commit produces the
# same bytes on any machine; CGO off means no glibc dependency in the image.
env GOOS="${GOOS_TARGET%%/*}" GOARCH="${GOOS_TARGET##*/}" CGO_ENABLED=0 \
    go build -trimpath -ldflags "-s -w" -o ./tmp/rooter .

echo "==> building ${IMAGE_REPO}:${TAG}"
docker build --progress=plain --platform="${GOOS_TARGET}" \
    -t "${IMAGE_REPO}:${TAG}" -f Dockerfile .

# Push a mutable :latest for the pinned-at-latest deploy pattern, plus an
# immutable sha tag so a bad rollout can be rolled back to a known image. Only
# tag with a sha when the worktree is clean, otherwise the tag would claim the
# image is something it isn't.
sha=""
if git rev-parse --git-dir >/dev/null 2>&1 && git diff-index --quiet HEAD -- 2>/dev/null; then
    sha="$(git rev-parse --short=7 HEAD)"
    docker tag "${IMAGE_REPO}:${TAG}" "${IMAGE_REPO}:${sha}"
else
    echo "==> worktree dirty, skipping the immutable sha tag"
fi

if [[ -n "${NO_PUSH}" ]]; then
    echo "==> NO_PUSH set, skipping push"
    docker images "${IMAGE_REPO}" --format 'table {{.Tag}}\t{{.Size}}'
    exit 0
fi

echo "==> pushing ${IMAGE_REPO}:${TAG}"
docker image push "${IMAGE_REPO}:${TAG}"
if [[ -n "${sha}" ]]; then
    echo "==> pushing ${IMAGE_REPO}:${sha}"
    docker image push "${IMAGE_REPO}:${sha}"
fi

echo "==> done: ${IMAGE_REPO}:${TAG}${sha:+ (and :${sha})}"
