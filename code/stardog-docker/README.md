# Docker-Stardog

This runs [Stardog 5 CE](http://www.stardog.com) in Docker. 

This image can be built either with or without a stardog license key 'baked in'. The image on Docker Hub (obviously) does *not* have a license key included, and one must be included in a stardog data volume that is attached to a running container at `/stardog`.

## To Build

1. (optiona) To build with a license key baked in, add `stardog-license-key.bin` to a new `resources` directory in the root of this repo.
2. `docker build --tag stardog:latest .`

## To Run

Note: If you get java segfault errors on run, try giving Docker more RAM ([more here](https://community.stardog.com/t/startup-error-running-stardog-5-0-1-in-docker/)). 

1.
```
docker run \
    -d -p 5820:5820 \
    --name stardog_db \
    -v [your local stardog data directory]:/stardog \
    stardog:latest
```

The `/stardog` volume is where Stardog persists its database, and where the license key must reside. If you're using an image with the license key baked in, you don't need to attach this volume - but you'll lose data with the image if you delete it!
