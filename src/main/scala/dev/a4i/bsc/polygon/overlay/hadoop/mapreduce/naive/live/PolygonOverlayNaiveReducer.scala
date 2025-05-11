package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.live

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.naive.model.TaggedGeometryWritable

type PolygonOverlayNaiveReducer = Reducer[Text, TaggedGeometryWritable, Text, Text]
