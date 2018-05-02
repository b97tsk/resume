# resume
Splitting download a file

# Install
```
go get -u github.com/b97tsk/resume
```

# Usage
```
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

# Advanced Usage
```
### Specify referer
$ echo "http://www.example.com/" >Referer
### Specify user agent
$ echo "Wget" >UserAgent
### Specify cookies
# echo -e ".example.com\tTRUE\t/\tFALSE\t1552218794\tNAME\tVALUE" >Cookies
```

# Files
* **URL** (mandatory) contains the URL you want to download.
* **Referer** (optional) contains the referer.
* **UserAgent** (optional) contains the user agent.
* **Cookies** (optional) contains the cookies. The file format is [Netscape format](https://unix.stackexchange.com/a/210282).
* **File** contains all the data downloaded from the URL.
If this file is missing, download will start from the beginning.
* **Hash** contains hash information about the File.
If this file is missing, download will start from the beginning.
* **ContentLength** contains the content length of the URL.
* **ContentMD5** contains the content MD5 of the URL if found in the response header.
* **ETag** contains the entity tag of the URL if found in the response header.
* **LastModified** contains the last modified time of the URL if found in the response header.
If ETag is found in the response header, this file will not be created.
