package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.live

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.TaggedGeometryWritable

type PolygonOverlayNaiveMapper = Mapper[LongWritable, Text, Text, TaggedGeometryWritable]
