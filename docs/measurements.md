# measurements

two identical-role boxes, each an RTX 3090 running the same Strata engine and model
(Q2_0 quant, 262144 context, `--kv int8`, `--batch 2`). Small prompts, 200 output tokens,
repeated. Your numbers will differ with the model, quant and context.

## one engine

| concurrent requests | per stream | aggregate |
|---|---|---|
| 1 | 114 tok/s | 114 tok/s |
| 2 | 63 tok/s | 122 tok/s |
| 4 | ~31 tok/s | 124 tok/s |

the aggregate is flat above 2 slots: decode is bounded by streaming experts from RAM over
PCIe, not by batching. adding a second GPU per box raises resident-expert capacity, not
tokens/s.

## two engines through the router

| conversations | per stream | aggregate |
|---|---|---|
| 2 (1 per box) | ~100 tok/s | ~200 tok/s |
| 4 (2 per box) | ~60 tok/s | ~245 tok/s |
| 8 (4 per box) | ~30 tok/s | ~245 tok/s |

so a second box is a ~2x throughput gain, not a 2x parallelism gain. four conversations at
60 tok/s is the sweet spot: an average agent turn of 1000 tokens takes ~17 s.

## cost of misrouting

cold prefill measured at ~1,385 tok/s:

| context | prefill |
|---|---|
| 32k | ~23 s |
| 131k | ~95 s |
| 262k | ~190 s |

engine-side cache restore is 5-6 ms per 1k tokens, i.e. ~1.4 s for a 262k conversation when
parking is enabled (`--conversation-cache-mib 32768 --conversation-cache-slots 8`). That is
the difference the router exists to avoid.

## memory

- live KV: ~13.7 KB per context token at int8 with KV streaming → 3.6 GB for a full 262k conversation
- parked conversation snapshot: ~19 KB/token → ~5 GB for 262k
- batch slots cost VRAM that the expert cache would otherwise use (docs: 0.56 GiB per slot at 32k context)
