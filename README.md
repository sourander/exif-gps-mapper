# Exif GPS Mapper

## Description

This package allows one to combine GPS data from Polar Flow exercises with the Photo timestamps from your local drive. Why? It can be useful to view where you have taken your images on map, especially when traveling abroad or adventuring outdoors.

The tool mines data from two sources:
* Polar Accesslink API (exercise summaries and route data)
* Your local drive (image EXIF data)

It generates three different tables:
* EXIF data from your images
  * filepath, timestamp, lens, (...)
* Exercise summary data
  *  id, transaction-id, start-time, start-time-utc-offset, (...)
* Route data
  * point_time, exercise_id, latitude, longitude.

The data is joined into a flat table with easy-to-use schema for visualization in a chosen software (Folium, Power BI, Tableau or other.) Example below:

|     | local_time               | local_time          | latitude  | longitude | ... |
|-----|--------------------------|---------------------|-----------|-----------|-----|
| 0   | D:\Images\2021\cat.NEF   | 2023-01-22 13:55:36 | 64.174200 | 27.600540 | ... |
| 1   | D:\Images\2023\dog.NEF   | 2023-01-22 13:55:51 | 64.174200 | 27.600540 | ... |
| 2   | D\Images\Archive\cow.NEF | 2023-01-22 13:59:22 | 64.174083 | 27.600893 | ... |
| ... | ...                      | ...                 | ...       | ...       | ... |

## How to use

I will write this when the script is a bit less work-in-progress. As of now, you need a custom-driver script for methods.