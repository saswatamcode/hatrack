# hatrack

```bash
A general purpose HA tracking proxy, that will drop requests from all HA ordinals apart from the active one.

Usage: hatrack [OPTIONS] --upstream-url <UPSTREAM_URL>

Options:
  -l, --listen-address <LISTEN_ADDRESS>
          Address/port to start proxy server on [default: :8080]
      --upstream-url <UPSTREAM_URL>
          Upstream URL to forward requests to
  -i, --internal-listen-address <INTERNAL_LISTEN_ADDRESS>
          Internal health server address for metrics/healthchecks [default: :8081]
      --inactive-window-seconds <INACTIVE_WINDOW_SECONDS>
          Duration of time after which HA ordinal is considered inactive [default: 30]
      --ordinal-grouping-header <ORDINAL_GROUPING_HEADER>
          Header name for HA ordinal grouping [default: cluster]
      --ordinal-header <ORDINAL_HEADER>
          Header name for HA ordinal [default: HATRACK-ORDINAL]
  -p, --possible-ordinals <POSSIBLE_ORDINALS>
          Possible ordinals for the HA tracker, these will show up as header values [default: prometheus-replica-0,prometheus-replica-1]
  -h, --help
          Print help
  -V, --version
          Print version
```