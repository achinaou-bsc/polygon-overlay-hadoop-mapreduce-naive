package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model

enum Counter extends Enum[Counter]:
  case BASE_POLYGON_READS
  case OVERLAY_POLYGON_READS
  case INTERSECTION_CHECKS
  case INTERSECTION_CALCULATIONS
  case MAP_OUTPUT_WRITES
  case REDUCE_OUTPUT_WRITES
