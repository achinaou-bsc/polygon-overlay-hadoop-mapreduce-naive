# Polygon Overlay / Hadoop MapReduce / Naive

- [Polygon Overlay / Hadoop MapReduce / Naive](#polygon-overlay--hadoop-mapreduce--naive)
  - [Algorithm](#algorithm)
  - [References](#references)

## Algorithm

```
method MAP (id, polygon p)
{
    extract source tag from p
    if p is from base layer then
        EMIT: (id, p)
    else
        for all base polygon id b do
            EMIT: (b, p)
        end for
    end if
}

method REDUCE (base polygon id, [c1, c2, ..])
{
    divide [c1, c2, ..] into set of overlay polygons C and a base polygon b
    M ← get overlay polygons from C overlapping with b
    for all c ∈ M do
        compute overlay (b, c)
        EMIT: output polygon
    end for
}
```

## Development

This project includes a [Development Containers](https://containers.dev) definition. If you open it in an environment that supports dev containers, such as [Visual Studio Code](https://code.visualstudio.com), you will be prompted to reopen the project in the dev container.

After the project opens in the dev container, wait for the initialization process to complete. Then, install the recommended extensions.

To open this project using Development Containers, the ports `8020`, `8032`, `8042`, `8088`, and `9870` should **not** be in use, as they are exposed to the host for accessing various Hadoop's management user interfaces.

Exposed Hadoop Management User Intefaces:
- [Name Node](http://localhost:9870)
- [Resource Manager](http://localhost:8088)
- [Node Manager](http://localhost:8042)

## References

- Puri, S. (2015). *Efficient Parallel and Distributed Algorithms for GIS Polygon Overlay Processing* (Doctoral dissertation, Georgia State University). https://doi.org/10.57709/7282021
