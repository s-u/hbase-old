volatiles <- new.env(parent=emptyenv())

.hcfg <- function() {
    if (is.jnull(volatiles$cfg)) {
       hh <- Sys.getenv("HADOOP_HOME")
       if (!nzchar(hh)) hh <- Sys.getenv("HADOOP_PREFIX")
       if (!nzchar(hh)) hh <- "/usr/lib/hadoop"
       hcmd <- file.path(hh, "bin", "hadoop")
       if (!file.exists(hcmd)) stop("Cannot find working Hadoop home. Set HADOOP_PREFIX if in doubt.")
       hb<- Sys.getenv("HBASE_HOME")
       if (!nzchar(hb)) hb <- Sys.getenv("HBASE_PREFIX")
       if (!nzchar(hb)) hb <- file.path(hh, "../hbase")
       hbcmd <- file.path(hb, "bin", "hbase")
       if (!file.exists(hbcmd)) stop("Cannot find working HBase home. Set HBASE_PREFIX if in doubt.")
       .jinit()
       hcp=strsplit(system(paste(shQuote(hcmd),"classpath"), int=T), ":", fixed=TRUE)[[1]]
       cp=strsplit(system(paste(shQuote(hbcmd),"classpath"), int=T), ":", fixed=TRUE)[[1]]
       cp=unique(unlist(lapply(c(hcp, cp), Sys.glob)))
       .jaddClassPath(cp)
       .jaddClassPath(system.file("java", package="hbase")) ## for our tools
       cfg=.jcall("org.apache.hadoop.hbase.HBaseConfiguration", "Lorg/apache/hadoop/conf/Configuration;", "create")
       if (is.jnull(cfg))
           stop("cannot load HBase configuration")
       volatiles$cfg <- cfg
    }
    volatiles$cfg
}

hbase.table <- function(name) {
    cfg <- .hcfg()
    t <- .jnew("org.apache.hadoop.hbase.client.HTable", cfg, as.character(name))
    structure(list(jobj=t, name=name), class="HBase_Table")
}

.restrict.get <- function(g, columns, family) {
    if (!missing(columns)) {
        if (missing(family)) { # use cf:column spec
            for (o in strsplit(as.character(columns), ":", TRUE)) {
                if (is.null(o[[2L]]))
                    .jcall(g, "Lorg/apache/hadoop/hbase/client/Get;", "addFamily", charToRaw(o[[1L]]))
                else
                    .jcall(g, "Lorg/apache/hadoop/hbase/client/Get;", "addColumn", charToRaw(o[[1L]]), charToRaw(o[[2L]]))
            }
        } else { # jsut use columns
            cf <- charToRaw(family)
            for (cn in columns)
                .jcall(g, "Lorg/apache/hadoop/hbase/client/Get;", "addColumn", cf, charToRaw(cn))
        }
    } else if (!missing(family))
        for (o in family)
            .jcall(g, "Lorg/apache/hadoop/hbase/client/Get;", "addFamily", charToRaw(o))
    g
}

hbase.get <- function(table, keys, columns, family) {
    if (is.character(table))
        table <- hbase.table(table)
    if (!inherits(table, "HBase_Table"))
        stop("invalid table object")
    if (missing(keys))
        stop("keys are mandatory")
    if (length(keys) < 1L)
        return(list())
    rc <- volatiles$res.convert
    if (is.jnull(rc)) rc <- volatiles$res.convert <- .jnew("Rpkg.hbase.HBResultTools")
    if (length(keys) == 1L) {
        g <- .restrict.get(.jnew("org.apache.hadoop.hbase.client.Get", charToRaw(keys)),
                           columns, family)
        res <- .jcall(rc, "[S", "newResult",
                      .jcall(table$jobj, "Lorg/apache/hadoop/hbase/client/Result;", "get", g))
        names(res) <- .jcall(rc, "[S", "columns")
        res
    } else {
        ## FIXME
        gets <- lapply(keys, function(key)
                       .restrict.get(.jnew("org.apache.hadoop.hbase.client.Get", charToRaw(key)),
                                     columns, family))
        l <- J("java.util.Arrays")$asList(.jarray(gets, "org.apache.hadoop.hbase.client.Get"))
        res <- .jcall(table$jobj, "[Lorg/apache/hadoop/hbase/client/Result;", "get", l)
    }
}
