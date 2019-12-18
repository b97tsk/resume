# resume

Splitting download a file.

# Install

```
go get -u github.com/b97tsk/resume
```

# Usage

```console
### Create a working directory.
$ mkdir work && cd work
### Download with max concurrent number set to 9.
$ resume -c 9 http://...
### A file named `File` will be created, save it when done.
$ cp File path/to/file
### Remove the working directory if you don't need it anymore.
$ cd .. && rm -rf work
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
- `output` output file.
- `per-user-agent-limit` limit per user agent connections.
- `range` request range.
- `read-timeout` read timeout.
- `referer` referer url.
- `remote-control` create an HTTP server for remote control.
- `response-header-timeout` response header timeout.
- `skip-etag` skip unreliable ETag field or not.
- `skip-last-modified` skip unreliable Last-Modified field or not.
- `split` split size.
- `stream-rate` maximum number of stream rate.
- `sync-period` sync-to-disk period.
- `tls-handshake-timeout` tls handshake timeout.
- `truncate` truncate output file before first write.
- `url` the URL you want to download.
- `user-agent` user agent, one for each line.
- `verify` verify output file after download completes.

> Note that command line arguments take precedence over this configure file.
