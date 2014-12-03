# US National Bridge Inventory (NBI) / OSM Import

This repository contains code to download data from Open Street Map (OSM) and match it against the US National Bridge Inventory, with the aim of improving routing data on OSM.

Two guiding principles are followed:

- NBI data is only used when a structure has already been tagged `bridge=yes` in OSM
- if a tag has already been specified on an OSM feature it is not altered

NBI data contains a variety of data. This code aims to collect three types:

- vertical clearances (both for ways on bridges and those passing under them)
- operating ratings, e.g. the allowable load on a bridge
- toll status
- NBI structure number

Additional data exists, which could be added to this import or collected at a later date. The NBI structure number (when paired with state FIPS code) is the NBI primary key, and would/can allow the collection of various data elements. Please see the PDF in this repository for a full list. Some of the more interesting elements are:

- horizontal clearances
- year built
- number of lanes on structure
- average daily traffic
- average daily truck traffic
- inspection date
- inspection frequency
- total project cost
- deck structure type


## Installation & Usage

**NOTE:** the postgres database `us_osm_nbi_import` will be repeatedly created & dropped during this operation, without notice. In the unlikely event that you already have a database with this name, do not use this script.

```
make setup
make load-osm
make load-nbi
make analyze
```

Individual states can be built with the following syntax, with `XX` replaced by the state's two-character abbreviation.

```
source .nhtsa_nbi_osm_import/bin/activate && python build_osm_nbi_import.py XX
```


## Output

*build/XX-unmatched.csv*
CSV file of NBI bridges in state `XX` that were not matched to OSM ways. May be useful for future tracing. Format is `FIPS Code,NBI Structure Number,WKT of NBI point in EPSG:4326`

*build/XX/YYYYY.json*
For each state XX, GeoJSON of the NBI bridge point; its matched way; and intersecting ways beneath it. Properties found in NBI are added to each feature's `properties` object, as appropriate. 

The length of intersecting ways is also included in the GeoJSON feature, and should be used as a filter for any import: longer ways should probably not have vertical clearances applied to them, because they will tend to intersect other ways that do not pass under the bridge. Such ways could be used for manual mapping tasks that split them into smaller ways, however. Alternately, an means of automatically trisecting the ways could be built.


## Caveats & Context

The NBI is maintained by the US Federal Highway Administration (FHWA). NBI staff has warned against relying upon their data for routing purposes, because it can outdated or miscoded. They suggest contacting individual state agencies for authoritative data.

However, the NBI data is extensive: in Florida, it contains ~12,000 bridge ways, versus ~8,000 in OSM. It is used by the US military for routing equipment, and by civil engineers to evaluate bridge lifespans and maintenance schedules. To the extent that it can be verified remotely, it appears to be of high quality.

More importantly, on net the addition of NBI data should improve routing derived from OSM even if it suffers from some flaws.

The reason for this is simple. There are two failure modes for routing using clearance restrictions:

1. A vehicle is sent along a route it cannot safely traverse
2. A vehicle is routed so as to avoid a more efficient route that it could have safely traversed

Of these, the first scenario is much more worrisome. OSM's coverage of `maxweight` and `maxheight` is quite poor in the US. For routing applications, the default assumption is generally that a way is passable unless a restriction is specified. The status quo is that drivers routed by OSM data must remain responsible for noticing and observing posted limits, and may well be routed to them.

As mentioned above, existing tags are not touched by this code. Therefore, only the second class of errors can be introduced by NBI data. Given the mostly-untagged status quo, the quality of routing should on net be improved by this import.

The significance of errors is also minimized by the tendency of adjacent bridge ways to be built so as to support a uniform underclearance. Consider the following example:

> http://bl.ocks.org/d/b91cd6d30b6556c08620

The highlighted bridge way is not closely matched to the NBI point. This may be because of imprecision in the NBI dataset or because the NBI treats as a single unit a structure that is composed of multiple ways in OSM. Whatever the case, the impact of the underclearance and operating load classifications are likely to be minimal: the structures over Halsey Ave will be built to support a uniform clearance, and 

One failure mode for the import is matching an OSM way to an imprecisely-positioned NBI bridge. This occurs 


## Known Bugs

- none, currently


## Contact

tlee (at) mapbox (dot) com


## License

MIT