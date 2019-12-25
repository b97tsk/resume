# resume

Splitting download a file.

# Install

```
go get -u github.com/b97tsk/resume
```

# Usage

```console
### Download with default options.
$ resume http://...
### Download with a specified output file.
$ resume -o dl.zip http://...
### Download with max parallel downloads set to 9.
$ resume -c9 http://...
### Download with a configure file (see below).
$ resume -f resume.yaml
### Download with default configure file (i.e. resume.yaml) found in current directory.
$ resume
### Print configure to standard output.
$ resume config
### Print status to standard output.
$ resume status
### Stream to standard output while downloading.
$ resume stream | mpv -
```

# Configure File

By default, a file named `resume.yaml` is read when the program starts, if it exists.
You can specify the path of this configure file by using command line flag `-f`.

A configure file is an YAML document which can specify following options:

- `alloc` alloc disk space before first write.
- `connections` maximum number of parallel downloads.
- `cookie` cookie file. The file format is [Netscape format](https://unix.stackexchange.com/a/210282).
- `dial-timeout` dial timeout.
- `errors` maximum number of errors.
- `interval` request interval.
- `keep-alive` keep-alive duration.
- `listen` HTTP listen address for remote control.
- `output` output file.
- `per-user-agent-limit` limit per user agent connections.
- `range` request range.
- `read-timeout` read timeout.
- `referer` referer url.
- `response-header-timeout` response header timeout.
- `skip-etag` skip unreliable ETag field or not.
- `skip-last-modified` skip unreliable Last-Modified field or not.
- `split` split size.
- `stream-rate` maximum number of stream rate.
- `sync-period` sync-to-disk period.
- `timeout` if non-zero, all timeouts default to this value.
- `tls-handshake-timeout` tls handshake timeout.
- `truncate` truncate output file before first write.
- `url` the URL to download.
- `user-agent` user agent, one for each line.
- `verify` verify output file after download completes.

> Note that command line arguments take precedence over this configure file.
