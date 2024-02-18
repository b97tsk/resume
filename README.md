# resume

A resumable multipart HTTP downloader.

# Install

```
go install github.com/b97tsk/resume@latest
```

# Usage

```console
### Download with default options.
$ resume http://...
### Download with a specified output file.
$ resume -o dl.zip http://...
### Download with max parallel downloads set to 9.
$ resume -c9 http://...
### Download with a config file (see below).
$ resume -f resume.yaml
### Download with default config file (i.e. resume.yaml) found in current directory.
$ resume
### Print config to standard output.
$ resume config
### Print status to standard output.
$ resume status
### Stream to standard output while downloading.
$ resume stream | mpv -
```

For a list of command-line flags, type `resume --help`.

# Config File

By default, a file named `resume.yaml` is read when the program starts, if it exists.
You can specify the path of this config file by using command-line flag `-f`.

A config file is an YAML document that can specify following options:

- `alloc` alloc disk space before the first write.
- `autoremove` auto remove .resume file after successfully verified.
- `connections` maximum number of parallel downloads (Default: 4).
- `cookie` cookie file. The file format is [Netscape format](https://unix.stackexchange.com/a/210282).
- `disable-keep-alives` disable HTTP keep alives, one request per connection.
- `errors` maximum number of errors (Default: 3).
- `force-http2` force attempt HTTP/2 (Default: true).
- `interval` request interval (Default: 2s).
- `keep-alive` keep-alive duration (Default: 30s).
- `limit-rate` limit download rate to this value.
- `listen` HTTP listen address for remote control.
- `max-split` maximal split size (MiB).
- `min-split` minimal split size (MiB).
- `output` output file.
- `proxy` a shorthand for setting http(s)\_proxy environment variables.
- `range` request range (MiB).
- `read-timeout` read timeout (Default: 30s).
- `referer` referer url.
- `response-header-timeout` response header timeout (Default: 10s).
- `skip-etag` skip unreliable ETag field or not.
- `skip-last-modified` skip unreliable Last-Modified field or not.
- `stream-cache` stream cache size (MiB).
- `stream-rate` maximum number of stream rate (MiB/s) (Default: 12).
- `sync-period` sync-to-disk period (Default: 10m).
- `timeout` timeout for all, low priority (Default: 30s).
- `timeout-intolerant` treat timeouts as errors.
- `tls-handshake-timeout` tls handshake timeout (Default: 10s).
- `truncate` truncate output file before the first write.
- `url` the URL to download.
- `user-agent` user agent.
- `verbose` write additional information to stderr.
- `verify` verify output file after download completes (Default: true).

Note that command-line arguments take precedence over this config file.

### Global Config File

In addition to this config file (`resume.yaml`), the program also tries to read
a global config file named `.resumerc` located at your home directory if it exists.
Options specified by this global config file have the lowest priority and therefore
can be overrided by `resume.yaml` or command-line arguments. The file format is the same.
