This project has been converted to a Flask Application https://github.com/smccaffrey/plex-etl

# Plex ETL Command Line Tool
This tool was originally designed for personal use, but after some refactoring efforts it will hopefully
make your media loading process into PLEX a lot easier.
## Installation
Change current directory to where you want to process media files.
```sh
$ cd /path/to/plex/libraries
```
Install plex_etl_cli
```sh
~/path/to/plex/libraries$ git clone https://github.com/smccaffrey/plex_etl_cli.git
```
## Usage - Movies
Intitialize the pipeline. You will be prompted to specify where your plex library lives.
```sh
$ python plex_etl.py --pipe movies --initialize
```
A directory called movies will be created in the cwd, with sub directories ```1_dump```, ```2_extracted```, ```3_transformed```, ```4_error```, ```5_encoding_queue```. Place all raw movie rips/torrents in ```1_dump```.

Extract all movie files.
```sh
$ python plex_etl.py --pipe movies --extract
```
Test if movies can be processed by the pipeline.
```sh
$ python plex_etl.py --pipe movies --test_parse
```
Transform all movie files
```sh
$ python plex_etl.py --pipe movies --transform
```
Load all transformed movies in ```PLEX_MEDIA_LIBRARY```
```sh
$ python plex_etl.py --pipe movies --load
```
Cleanup all ETL directories in the movies pipeline
```sh
$ python plex_etl.py --pipe movies --cleanup
```


## Versions

### v1.0.1
* added ```cleanup()``` function remove old directories and file from ```1_dump``` and ```2_extracted``` from the ```movies``` pipe

### v1.0.0
* Created initial functionality
* Fully supports processing movie files

