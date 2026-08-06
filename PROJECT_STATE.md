# Project State

## Current milestone

Phase 1B - Local readiness and data-contract finalization in progress.

## Current status

- `AGENTS.md` is the active repository instruction file.
- Git has been initialized on the `main` branch.
- The initial controlled foundation has been preserved through Git.
- Python 3.12.10 is installed and available.
- Eclipse Temurin OpenJDK 17.0.20 is installed and `JAVA_HOME` is configured.
- Docker Desktop 4.84.0 and Docker Engine 29.6.2 are operational.
- Docker Compose 5.3.1 and the WSL2 Linux backend are available.
- No application code has been written.
- No dependencies have been installed.
- No NASA data has been downloaded.
- No AWS resources have been created.

## Approved mission

Build a professional batch and streaming data-engineering platform that processes approximately 10 million NASA-derived event messages using clearly distinguished original NASA records, enriched records, replay events, and synthetic scale-test records.

## Current gate

Local prerequisites are verified. The canonical event, NASA source, and Bronze contracts must be finalized before extraction implementation begins.

## Next proposed milestone

Phase 1B - verify local prerequisites and finalize the canonical event and Bronze contracts before implementing NASA extraction.

## Known constraints

- The laptop has 16 GB RAM; Docker currently has approximately 7.6 GB available.
- The project workspace is on a mechanical D: drive, so Spark shuffle and file-heavy operations may be slower.
- The 5-million and 10-million execution environments will be chosen from measured earlier-gate results.
- AWS deployment is prohibited until the local 10,000-record vertical slice passes.
- Major dependencies require approval before installation.

## Integrity reminder

The project must never describe all 10 million processed event messages as original NASA observations.
