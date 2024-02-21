# Public transport isochrones with GTFS and OpenStreetMap

This repository contains the code for the calculation of isochrones using the data from OpenStreetMap for the street network and GTFS for public transport.

In the notebook there is an example using the data of Torino and the GTFS of Gruppo Torinese Trasporti.
Before running the notebook, you should download the street network graph (`osmnx` is the library I used, for the `walk` type network graph) and simplify it a bit (it's approximately 190k nodes).

Also, download the GTFS data from [aperto.comune.torino.it](http://aperto.comune.torino.it/), and remove the stops (and the corresponding trips) which are not in the area downloaded from OpenStreetMap.

Then, using the notebook you can compute the time taken from a certain point, starting at a specified date and time.

## Isochrone image generation
With the output of the algorithm, the isochrones map can be created. Several isochrones are available in the `images` folder, showing the time taken to reach any point from the Porta Nuova station at different times during the day (20th January 2024). 

![Isochrone of Torino](images/isochrone_portanuova_2024-01-20_11%3A30%3A00.png)

Here, the layers correspond, respectively, from 10 to 50 minutes with increment of 5 minutes at each layer.
