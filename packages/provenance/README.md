# Experimental outer provenance bridge

`bind_supplied_provenance` compares complete supplied acquisition/task/output values
and full calculation content against an explicitly retained original R9 receipt.
It performs no acquisition or backend I/O and never marks a generation complete.

`verify_live_generation` is a separate explicit operational API. It invokes the
accepted GenerationStore read path with the exact supported DuckDB sinks. Waiting
is not full readback. Only complete verified expected inventory sets its returned
operational flag. Reports are data, not authenticated execution capabilities.

This outer package requires the exact accepted worker/SDK/DuckDB dependencies,
including native DuckDB/NumPy. Pure contracts/evidence/diagnostics remain independent
and never import it. Initial profile is one-entity trades and the five unchanged
compute_trades outputs. No new source catalog, wire codec, provider rights,
historical acquisition authentication, hard deadline or physical quota is claimed.

The implementation and native/source/archive/installed qualification are candidates
until the linked EQ103 acceptance completes. See docs/stories/EQ-103_PLAN.md in the core
repository for literal bounds, trust limits and the qualification protocol.
