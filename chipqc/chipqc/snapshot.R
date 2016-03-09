#!/usr/bin/Rscript
options(stringsAsFactors = FALSE)

suppressMessages(library(ggplot2))
library(Gviz)
library(biomaRt)
library(rtracklayer)

args <- commandArgs(trailingOnly = TRUE)

outdir <- "."
file_bw <- "x"
external_id <- "x"
genome <- "hg38"
martENSEMBL <- useMart(host='www.ensembl.org', biomart='ENSEMBL_MART_ENSEMBL', dataset='hsapiens_gene_ensembl')
regions_bed <- ""



i <- 1
while(i < length(args)){
    if( args[i] == '--out-dir'){
        i <- i+1
        outdir  <- args[i]
    } else if( args[i] == '--file-bw'){
        i <- i+1
        file_bw  <- args[i]
    } else if( args[i] == '--external-id'){
        i <- i+1
        external_id  <- args[i]
    } else if( args[i] == '--grch37'){
        genome <- "hg19"
        martENSEMBL <- useMart(host='grch37.ensembl.org', biomart='ENSEMBL_MART_ENSEMBL', dataset='hsapiens_gene_ensembl')
    } else if(args[i] == '--regions-bed'){
        i <- i+1
        regions_bed  <- args[i]
    }
    i  <- i+1
}

reg <- read.delim(regions_bed,sep="\t",header=F)
if (ncol(reg) == 3){
    reg$V4 <- paste(reg[,1],"_",reg[,2],"-",reg[,3],sep="")
    reg$title <- paste(reg[,1],":",reg[,2],"-",reg[,3],sep="")
} else if (ncol(reg) == 4) {
    reg$title <- reg$V4
}

gr <- import(file_bw,which = GRanges(reg[,1], IRanges(reg[,2], reg[,3])))

for ( reg_i in 1:nrow(reg) ){
  ideoTrack <- IdeogramTrack(genome = genome, chromosome = reg[reg_i,1],  from = reg[reg_i,2], to = reg[reg_i,3])
  BWtrack <- DataTrack(range = gr, genome = genome, type = "l", chromosome = reg[reg_i,1], name = external_id)
  axisTrack <- GenomeAxisTrack(name=reg[reg_i,1])

  biomTrack <-
    BiomartGeneRegionTrack(genome = genome, chromosome = reg[reg_i,1], start = reg[reg_i,2], end = reg[reg_i,3],
        biomart=martENSEMBL,
        name = "ENSEMBL", strand='*', collapseTranscripts = "longest",
        col.line = NULL, col = NULL,
        filters=list(biotype="protein_coding"),
        stacking = "pack", showId=TRUE)

  out_file <- paste(outdir,"/",external_id,"_",reg[reg_i,4],".png",sep="")
  print(out_file)
  png(out_file, width=1024, height=1024)
  plotTracks(list(ideoTrack, axisTrack, BWtrack, biomTrack), chromosome = reg[reg_i,1],  from = reg[reg_i,2], to = reg[reg_i,3], type = "l")
  dev.off()
}
