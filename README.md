<img src="https://raw.githubusercontent.com/FanHarvard/experiment-core/main/docs/icon.png" alt="Experiment core: a central module connecting experimental operations" width="64" height="64">

# experiment-core

**Still in active development.**

Small, domain-neutral contracts and a local runtime for supervised experiments:
bounded operations, resource ownership, durable intent/result records,
cancellation, explicit approvals and immutable artifacts.

```sh
pip install experiment-core
```

Python 3.10+; depends only on `jsonschema`. No LDCon, agentbridge, provider CLI
or hardware dependency. Host applications supply authentication and physical
device adapters; unknown writes are never replayed automatically.

Tests cover schemas, lifecycle transitions, resource conflicts, cancellation,
approval/adoption and recovery of recorded operations. Run `python -m pytest -q`.
The process-locking runtime is tested on Linux.

[JSON examples](examples/) · [Publishing](docs/publishing.html) · [MIT license](LICENSE)
