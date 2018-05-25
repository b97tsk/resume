# resume
Splitting download a file

# Install
```
go get -u github.com/b97tsk/resume
```

# Usage
```console
### Create a working dir
$ mkdir work && cd work
### Create a URL file
$ echo "http://www.example.com/file" >URL
### Download with max concurrent number set to 9
$ resume -c 9
### A file named File will be created, save it when done
$ cp File path/to/file
### Remove the working dir if you don't need it anymore
$ cd .. && rm -rf work
```

# Files
### Input Files
- **URL** (optional) contains the URL you want to download.
- **Referer** (optional) contains the referer.
- **UserAgent** (optional) contains the user agent.
- **Cookies** (optional) contains the cookies. The file format is [Netscape format](https://unix.stackexchange.com/a/210282).
> Argument #1 is considered the URL, if provided, the **URL** file will be ignored.
### Output Files
- **File** contains all the data downloaded from the URL.
- **Hash** contains hash information of the **File**.
> If **File** or **Hash** is missing, download will start from the beginning.
