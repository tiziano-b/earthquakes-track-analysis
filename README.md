# Earthquakes tracker

The solution is used to store and visualize earthquakes' information.<br/>
It allows the final user to view all the specific details of the earthquake, the detail of the closest city affected by the earthquake, the image of the wave recorded by the near Seismometer and if exist, news article related to the specific earthquake.<br/> 
In addition, it extracts and shows from the identified articles the sentences related to death or injuries.

## Getting Started

The solution has been developed using Ambari.<br/>
This repository has only a small part of the solution, a HIVE SQL script (with related Java UDFs) to merge together data related to a dataset of earthquakes.<br/>
The script identifies the closest City and Seismometer affected by the earthquake.

### Prerequisites

Ambari.<br/>
Datasets of: Earthquakes, cities, Seismometers.

## Built With

* [Ambari](https://ambari.apache.org) - Hadoop management


