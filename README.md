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

Additional data exists, which could be added to this import or collected at a later date. The NBI structure number (when paired with state FIPS code) is the NBI primary key, and would/can allow the collection of the following data elements:



## Caveats

The NBI is maintained by the US Federal Highway Administration (FHWA). NBI staff has warned against relying upon their data for routing purposes, because it can outdated or miscoded. They suggest contacting individual state agencies for authoritative data.

However, the NBI data is extensive: in Florida, it contains ~12,000 bridge ways, versus ~8,000 in OSM. It is used by the US military for routing equipment, and by civil engineers to evaluate bridge lifespans and maintenance schedules. To the extent that it can be verified remotely, it appears to be of high quality.

More importantly, on net the addition of NBI data should improve routing derived from OSM even if it suffers from some flaws.

The reason for this is simple. There are two failure modes for routing using clearance restrictions:

1. A vehicle is sent along a route it cannot safely traverse
2. A vehicle is routed so as to avoid a more efficient route that it could have safely traversed

Of these, the first scenario is much more worrisome. OSM's coverage of `maxweight` and `maxheight` is quite poor in the US. For routing applications, the default assumption is generally that a way is passable unless a restriction is specified. The status quo is that drivers routed by OSM data must remain responsible for noticing and observing posted limits, and may well be routed to them.

As mentioned above, existing tags are not touched by this code. Therefore, only the second class of errors can be introduced by NBI data. Given the mostly-untagged status quo, the quality of routing should on net be improved by this import.

The significance of errors is also minimized by the tendency of adjacent bridge ways to be built so as to support a uniform underclearance. Consider the following example:

http://bl.ocks.org/d/b91cd6d30b6556c08620

The highlighted bridge way is not closely matched to the NBI point. This may be because of imprecision in the NBI dataset or because the NBI treats as a single unit a structure that is composed of multiple ways in OSM. Whatever the case, the impact of the underclearance and operating load classifications are likely to be minimal: the structures over Halsey Ave will be built to support a uniform clearance, and 

One failure mode for the import is matching an OSM way to an imprecisely-positioned NBI bridge. This occurs 

## Known Bugs

- none!