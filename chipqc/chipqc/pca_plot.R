#!/usr/bin/Rscript
options(stringsAsFactors = FALSE)
require(RSQLite)
require(reshape)
require(ggplot2)
require(RColorBrewer)
getPalette = colorRampPalette(brewer.pal(9, "Set1"))

args <- commandArgs(trailingOnly = TRUE)
outdir <- "."
dbfile <- "x"

a_key="CELL_LINE"
b_key="LIBRARY_STRATEGY"

i <- 1
while(i < length(args)){
    print(i)
    if( args[i] == '--out-dir'){
        i <- i+1
        outdir  <- args[i]
    } else if( args[i] == '--db-file'){
        i <- i+1
        dbfile  <- args[i]
    } else if( args[i] == '--key-a'){
        i <- i+1
        a_key  <- args[i]
    } else if(args[i] == '--key-b'){
        i <- i+1
        b_key  <- args[i]
    }
    i  <- i+1
}

if (outdir == ".") {
    throw("No OUPTUT directory provided")
}
if (dbfile == "x") {
    throw("No DB file provided")
}

outdir <- paste(outdir,"/",sep="") ## make sure there is a / at the end
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

print(outdir)

run_plot <- function(con, output_dir, annot_a, annot_b, b_color, file_extention){
    a_opt <- unique(annot_a$value)
    for ( a_string in a_opt) {
        print(paste("Process", a_string, "..."))
        a_ids <- subset(annot_a, value == a_string, did)$did

        corr <- dbGetPreparedQuery(con,"
            SELECT did_a, did_b, out from correlation
            WHERE status = 'done'
            and (did_a in (SELECT did from data_annotation where value = ? )
                 and did_b in (SELECT did from data_annotation where value = ? ));", data.frame(x=a_string,y=a_string))

        print(nrow(corr))

        if(nrow(corr) <= 0){
          next
        }
        corr$out <- as.double(corr$out)
        names(corr) <- c("a","b","value")
        ids <- sort(unique(c(corr$a,corr$b)))

        ids_len <- length(ids)
        mtx <- matrix(nrow=ids_len,ncol=ids_len)
        rownames(mtx) <- ids
        colnames(mtx) <- ids

        for (a in 1:ids_len){
          for (b in 1:ids_len){
            aid<- ids[a]
            bid<- ids[b]
            if(a == b){
              mtx[a,b] = 1
            } else if(a < b){
              df <- corr[corr$a == aid & corr$b == bid,]
              val <- df$value
              mtx[a,b] <- val
              mtx[b,a] <- val
            } else if(a > b){
              df <- corr[corr$a == aid && corr$b == bid]
              if(length(df) > 0){
                print(paste(a,b))
              }
            }
          }
        }
        fit <- princomp(mtx, cor=TRUE)
        load <- fit$loadings[,1:2]

        lctype <- annot_b[match(rownames(load),annot_b$did),]$value

        pca<-data.frame(load[,1],load[,2],lctype)
        names(pca) <- c("load_a","load_b","cell_type")
        for(query_id in a_ids){
            query_external_id <- id_to_extid[match(query_id,id_to_extid$did),]$external_id

            pcatitle <- paste("Type: `",a_string,"` (",query_external_id,")",sep="")

            file<-paste(output_dir,"/",query_external_id,"-",file_extention,".png",sep="")
            print(file)
            png(file,width=1546, height=800, res=120)

            p <- ggplot(pca, aes(x = load_a, y = load_b)) +
            geom_point(data=pca[match(query_id,rownames(pca)),], aes(x = load_a, y = load_b), colour="black", size=7)+
            geom_point(aes(colour=cell_type), size=4) +
            scale_colour_manual(values = b_color)+
            ggtitle(pcatitle) +
            theme(plot.title = element_text(lineheight=.8, face="bold"))
            print(p)
            dev.off()
        }
    }
}
con <- dbConnect(SQLite(), dbname=dbfile)
id_to_extid <- dbGetQuery(con,"SELECT did, external_id FROM data_file;")
annot_type_a <- dbGetPreparedQuery(con,"SELECT a.did, a.kid, a.value from data_annotation a where a.kid in (SELECT kid from data_key where key = ?);", data.frame(x=a_key))
annot_type_b <- dbGetPreparedQuery(con,"SELECT a.did, a.kid, a.value from data_annotation a where a.kid in (SELECT kid from data_key where key = ?);", data.frame(x=b_key))

opt_type_a <- unique(annot_type_a$value)
opt_type_b <- unique(annot_type_b$value)

color_type_a <- getPalette(length(opt_type_a))
names(color_type_a) <- opt_type_a

color_type_b <- getPalette(length(opt_type_b))
names(color_type_b) <- opt_type_b

#a_dir <- paste(outdir,a_key,sep="")
#dir.create(a_dir, showWarnings = FALSE)

run_plot(con=con, output_dir=outdir, annot_a = annot_type_a, annot_b = annot_type_b, b_color = color_type_b, file_extention="")

#b_dir <- paste(outdir,a_key,sep="")
#dir.create(b_dir, showWarnings = FALSE)
#run_plot(con=con, output_dir=b_dir, annot_a = annot_type_b, annot_b = annot_type_a, b_color = color_type_a, file_extention="lib")

dbDisconnect(con)
