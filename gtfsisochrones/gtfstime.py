import polars as pl

map_days={i:k for i,k in enumerate(("monday","tuesday","wednesday","thursday","friday","saturday","sunday"))}

class GtfsTime:
    def __init__(self, hours=0, minutes=0, seconds=0):
        self.hours = hours
        self.mins = minutes
        self.secs = seconds

    def __add__(self, other):
        total_seconds = self.total_seconds() + other.total_seconds()
        return GtfsTime.from_seconds(total_seconds)
    
    def __sub__(self, other):
        total_seconds = self.total_seconds() - other.total_seconds()
        return GtfsTime.from_seconds(total_seconds)

    def total_seconds(self):
        return self.hours * 3600 + self.mins * 60 + self.secs
    
    def diff_second(self, other):
        return self.total_seconds() - other.total_seconds()

    @classmethod
    def from_seconds(cls, total_seconds):
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return cls(int(hours), int(minutes), int(seconds))
    
    @classmethod
    def from_string(cls, string):
        hours, minutes, seconds = string.split(":")
        return cls(int(hours), int(minutes), int(seconds))

    def __repr__(self):
        return f"GtfsTime(hours={self.hours}, minutes={self.mins}, seconds={self.secs})"

    def __str__(self):
        return f"{self.hours:02d}:{self.mins:02d}:{self.secs:02d}"
    

def add_time(gtfsTime,seconds):
    return GtfsTime.from_seconds(gtfsTime.total_seconds()+seconds)

def transform_date_str(date):
    return "".join(date.isoformat().split("-"))

def get_trips_date(calendar, calen_dates, gtfstrips, trips_metro, pydate_start):

    dayofweek = map_days[pydate_start.weekday()]
    date_start=transform_date_str(pydate_start)
    calen_today=calen_dates.filter(pl.col("date")==date_start)

    exc={i: calen_today.filter(pl.col("exception_type")==i)["service_id"] for i in [1,2]}

    active=calendar.filter((pl.col("start_date")<=date_start)&(pl.col("end_date")>=date_start)&(pl.col(dayofweek)==1))

    service_ids_active = set(active["service_id"]).union(set(exc[1])).difference(set(exc[2]))

    trips_today = gtfstrips.lazy().filter(
        pl.col("service_id").is_in(service_ids_active)
        ).with_columns(
        pl.col("trip_id").is_in(trips_metro).alias("is_metro")).collect()
    
    tt=trips_today[["trip_id","is_metro"]].unique()

    return tt