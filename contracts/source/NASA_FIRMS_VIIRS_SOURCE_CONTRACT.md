# NASA FIRMS VIIRS Source Contract

## Status

Approved baseline for Phase 1. Any material source change requires an engineering decision and contract update.

## Official source

NASA Fire Information for Resource Management System (FIRMS), Visible Infrared Imaging Radiometer Suite (VIIRS) active-fire and thermal-anomaly detections.

The platform must describe these records as satellite detections or thermal anomalies. It must not automatically describe each record as a confirmed wildfire or fire incident.

## Approved products

| Use | Product identifier | Purpose |
|---|---|---|
| Reproducible historical processing | `VIIRS_SNPP_SP` | Standard-processing historical source |
| Incremental demonstration | `VIIRS_SNPP_NRT` | Near-real-time source with documented revision limitations |

Near-real-time records may later be superseded by standard-processing records. Bronze must preserve the product identifier and version so revisions are auditable.

## Access methods

- Historical processing: bounded NASA FIRMS archive extracts.
- Incremental processing: bounded NASA FIRMS Area API requests.
- Authentication: `NASA_FIRMS_MAP_KEY` supplied through the ignored local `.env` file or an approved cloud secret store.
- The key must never appear in Git, logs, screenshots, reports, or event payloads.

## Expected source fields

The source contract expects the applicable VIIRS fields below. NASA may add fields; unknown fields must be preserved in Bronze and assessed before entering Silver.

| Source field | Meaning | Canonical handling |
|---|---|---|
| `latitude` | Detection latitude | Validate from -90 through 90 |
| `longitude` | Detection longitude | Validate from -180 through 180 |
| `bright_ti4` | VIIRS I4 brightness temperature | Preserve source value; normalize unit naming in Silver |
| `scan` | Along-scan footprint dimension | Preserve and validate as positive when present |
| `track` | Along-track footprint dimension | Preserve and validate as positive when present |
| `acq_date` | Acquisition date in UTC | Combine with `acq_time` |
| `acq_time` | Acquisition time in UTC | Preserve leading zeros and combine with date |
| `satellite` | Satellite identifier | Preserve source value |
| `instrument` | Instrument identifier | Expected to represent VIIRS for this contract |
| `confidence` | Source confidence category | Preserve and validate against profiled source values |
| `version` | Source product version | Preserve exactly |
| `bright_ti5` | VIIRS I5 brightness temperature | Preserve source value; normalize unit naming in Silver |
| `frp` | Fire radiative power | Preserve; validate as non-negative when present |
| `daynight` | Day/night indicator | Validate as `D` or `N` |

Additional source fields must be preserved in the immutable raw object even when they are not yet part of the canonical schema.

## Request boundaries

Every extraction request must be bounded by:

- Product identifier
- Geographic area or explicit worldwide scope
- Start date
- End date or permitted day range
- Ingestion run ID

Unbounded historical extraction is prohibited.

## Source manifest

Every successful or failed request must record:

- `ingestion_run_id`
- Source dataset and product
- Request time in UTC
- Requested geography
- Requested date range
- HTTP outcome
- Raw object location when created
- Raw filename
- Raw byte count
- Raw checksum
- Source record count when readable
- Pipeline version
- Failure category when unsuccessful

## Source identity

FIRMS CSV records do not provide a universally guaranteed unique record ID. The canonical `source_record_id` must therefore be a deterministic identity derived from approved stable source attributes.

The identity definition must include at least:

- Source dataset
- Satellite
- Acquisition date
- Acquisition time
- Source latitude
- Source longitude
- Source product version

### Version 1 identity algorithm

The implemented identity version is `nasa-firms-viirs-v1`.

1. Trim the source dataset, satellite, acquisition date, and product version.
2. Normalize acquisition time to four-digit `HHMM` and validate clock bounds.
3. Normalize latitude and longitude with decimal arithmetic, removing insignificant trailing zeros and converting negative zero to zero.
4. Construct a JSON object containing the identity version and the approved identity fields.
5. Serialize the object with keys sorted and no insignificant whitespace.
6. Calculate SHA-256 over the UTF-8 serialization.
7. Format the source identity as `nasa-firms-viirs-v1:sha256:<digest>`.

Equivalent decimal and acquisition-time representations must produce the same identity. A different source dataset or product version must produce a different identity.

## Failure behavior

- A partial response must not receive a successful manifest status.
- A checksum mismatch must fail the ingestion run.
- A changed or missing required source field must fail contract validation.
- Raw bytes must remain available for diagnosis when safely obtained.
- Deterministic source errors must not be retried indefinitely.
- Transient network and server failures may receive bounded retries.

## Acceptance criteria

The source contract passes when a bounded representative NASA extract can be traced to one manifest, its checksum is verified, its record count is known, and its source fields are profiled without altering the original file.
