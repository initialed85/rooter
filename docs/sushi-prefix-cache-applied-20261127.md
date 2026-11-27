# sushi prefix-cache sizing: applied, with the numbers before and after

Requested by pi-bot (delegation `24137d11-77de`), authorised by Ed in Chat at ~19:20 AWST
("i'll approve whatever you want re: sushi"). Scope as given: entry cap and SSD budget plus the
restart itself. Routing, affinity and tier policy untouched. strata not restarted. No home-ops file
touched by me.

## What changed

`~/run-sushi.sh` (Ed's own launch script, on the Mac at 192.168.137.111):

| knob | before | after | how it is set |
|---|---|---|---|
| hot entry cap | 4 | 12 | `--prefix-cache-entries 12` |
| SSD budget | 4,294,967,296 | 8,589,934,592 | `--prefix-cache-disk 8GB` |
| hot byte budget | 4,294,967,296 | unchanged | `--prefix-cache-mem 4GB` |

`/props` has no field for the entry cap. It is visible only in the process command line; the
`disk_entries` number is a live count of SSD-resident entries, not a cap, and it moved 4 → 5 with no
restart, which is what proved it to me.

## Window chosen, gate used, restart stamps

I picked the window; pi-bot's only condition was that I name it. Gate: four consecutive samples,
15 s apart, of rooter `inflight = 0` with no `oldestInflightMs` key (rooter's own accounting — the
one every fleet request passes through). sushi's `vllm:num_requests_running` was sampled too and is
**not** sufficient on its own: it read 0 across four samples while four of my own requests were
demonstrably in flight, so it cannot be the idleness signal.

- 12:06:00Z — pre-stop snapshot taken, box drained.
- `KEEP_WIRED=1 DRAIN=90 ~/stop-sushi.sh` → `SIGTERM, draining up to 90s`, `server stopped`,
  `port 8088 closed`, `wired limit 60416 MB · 13.0 GB pages free`.
- 12:07:08Z — relaunched in the same tmux pane (`0:0.2`) as pid 29381, log `~/sushi-restart-20261127T2000.log`.
- 12:07:28Z — verified: `--prefix-cache-entries 12 --prefix-cache-mem 4GB --prefix-cache-disk 8GB`,
  `"disk_bytes":8589934592`, `"mem_bytes":4294967296`, `/health {"status":"ok"}`,
  `iogpu.wired_limit_mb 60416`, rooter `sushi healthy=True slotsFree=4`.
- Rooter served throughout; request rate did not dip across the window.

## Before and after

Baseline (cumulative, 16 d 10 h 53 m of uptime, sampled 12:06:00Z):

```
hot restores 10,365   hot evictions 1,308   ratio 7.9 : 1
SSD restores     66   SSD evictions 3,799   ratio 0.017 : 1
requests 11,198  (28.4 / hour)      disk_used 4,201,208,903 / 4,294,967,296 = 97.8% of budget
prefix_cache_write_bytes 4.40 TB since Nov 11  (~268 GB / day)
```

Post-restart (12:07:08Z → 13:10:34Z = 63.4 min, includes five requests from my own probe):

```
hot restores 143   hot evictions 6    ratio 23.8 : 1
SSD restores   2   SSD evictions 18   ratio 0.11 : 1     (one of those 2 SSD hits was my probe)
requests 152  (143 / hour, ~5x the baseline hourly rate)
query-level hit rate 132/141 = 93.6%  (baseline 10,406/11,198 = 92.9%)
disk_used 5.24 – 8.56 GB, oscillating against the new 8 GiB ceiling; it passed the old 4.29 GiB
   budget within ~15 minutes and stayed above it
disk_written 31,069,016,691 in 63 min = 29.4 GB / hour = ~706 GB / day
```

## The plain answer, including the part that is not yet a result

- **The tier behaviour changed, measurably and immediately.** The SSD tier now holds ~8 GiB instead
  of 4.3 and is running at its ceiling (used bytes oscillate 5.2 → 8.6 GB, `disk_entries` 5–10). It
  could not do that before the change, on either knob.
- **Hot-tier pressure eviction went down in ratio terms**: 7.9 restores per eviction before, 23.8
  after. In absolute rate, hot evictions went 3.3/hour → 5.7/hour while traffic went 28.4 → 143
  requests/hour, so the honest reading is *evictions did not scale with traffic*, which is what the
  larger cap was for.
- **Whether the SSD tier now pays for itself is not yet measurable.** Normalised, SSD restores went
  0.17/hour → 1.9/hour, which looks like 11×, but it is two events, one of them mine: the real
  number for the fleet in the first hour is **one SSD restore**. Say it plainly: as of 13:10Z the
  change has demonstrably made the SSD tier bigger and busier, and has **not** yet demonstrably made
  it profitable. Judge it tomorrow on `sushi:prefix_cache_tier_hits_total{tier="ssd"}` against
  `{tier="ssd"}` evictions over a comparable traffic day.
- **The cost side is not a null result — it went up.** SSD writes are ~706 GB/day post-restart
  against ~268 GB/day measured over 16.4 days pre-restart: roughly **2.6× the write rate**. That is
  what a bigger budget does: more entries land, more landings get written before they are read.
  `~/.sushi/kv-cache` is now **11 GB** and `/System/Volumes/Data` reads **832 GiB used of 926 GiB,
  57 GiB available (94% full)** — tighter than the 72.4 GB free I measured at 11:37Z. If that
  headroom matters to Ed, the budget should be treated as a claim on volume space, not free capacity.

## The one clean win in the window

My 33,761-token probe prefix (written cold at 12:09:50Z) was written to SSD during its cold prefill,
was served from hot at +12.4, +30.4 and +44.7 min, and was **served from SSD at +56.7 min** after
its hot entry had gone: `ssdHits` 1 → 2 with `hotHits` unchanged, `prompt_ms 871.164`,
`cached_tokens 33730`. Under the old 4-entry / 4 GiB box that eviction was the end of the prefix's
usefulness; here the second tier caught it exactly once, which is the mechanism the sizing change
was bought for. That is one prefix, so it is a mechanism observed, not a rate.
