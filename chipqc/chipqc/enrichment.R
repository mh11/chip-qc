args <- commandArgs(trailingOnly = TRUE)
#print(args)

dbfile <- ''
outdir <- ''

i <-1

while(i <= length(args)){
    if(args[i] == '--db'){
        i <- i+1
        dbfile <- args[i]
    } else if(args[i] == '--out'){
        i <- i+1
        outdir  <- args[i]
    }
    i  <- i+1
}

library("RSQLite")

#print (c(dbfile,outdir))

tmp <- dbConnect(SQLite(), dbname=dbfile)
summary(tmp)
dbDisconnect(tmp)
