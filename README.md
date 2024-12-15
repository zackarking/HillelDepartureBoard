# HillelDepartureBoard
Make sure you have the requirements.txt installed.
Currently just displays all MARC trains running at the time. Run as follows:
```bash
./arrivals.py --marc_gtfs ./mdotmta_gtfs_marc 
```

Notes for next steps:
- [x] Metro API
- [ ] Parse MARC schedule and combine with MARC realtime
- [ ] Python write to HTML
- [ ] Find library (selenium?) for python to open a page in the browser,
make it fullscreen, and refresh the page
- [ ] Script to git pull update
- [ ] Script to check for updates to MARC GTFS schedule and pull new files
- [ ] Script to turn off screen at night
- [ ] Find which browsers work well with the Pi
