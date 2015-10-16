#!/usr/bin/Rscript

getArgs <- function(verbose=FALSE, defaults=NULL) {
  myargs <- gsub("^--","",commandArgs(TRUE))
  setopts <- !grepl("=",myargs)
  if(any(setopts))
    myargs[setopts] <- paste(myargs[setopts],"=notset",sep="")
  myargs.list <- strsplit(myargs,"=")
  myargs <- lapply(myargs.list,"[[",2 )
  names(myargs) <- lapply(myargs.list, "[[", 1)

  ## logicals
  if(any(setopts))
    myargs[setopts] <- TRUE

  ## defaults
  if(!is.null(defaults)) {
    defs.needed <- setdiff(names(defaults), names(myargs))
    if(length(defs.needed)) {
      myargs[ defs.needed ] <- defaults[ defs.needed ]
    }
  }

  ## verbage
  if(verbose) {
    cat("read",length(myargs),"named args:\n")
    print(myargs)
  }
  myargs
}


args <- getArgs(defaults=list(ip="", input="", ipsz=0, inputsz=0, outfile="", outdir=".", keep.files=FALSE, plot=FALSE, counts=FALSE, bins = "1000bpregions.bed", path="", sleep=15))

    ##Function to calculate IP enrichment statistics, modified from CHANCE (Diaz A, Nellore A and Song JS 2012).

if(args$ip == "" | args$input == "")
{  
	cat("IP and Input files must be provided!\n\n");  

	cat("Function to calculate IP enrichment statistics, modified from CHANCE (Diaz A, Nellore A and Song JS 2012).
Options:
  ip is path to IP file, either bigWig (default) or a counts file (see below) (required)
  input is path to Input (control) file, either bigWig (default) or a counts file (see below) (required)
  ipsz is the number of reads used to create the IP file. For Blueprint this is unique_reads_post_filter (optional, used for calculating z-score)
  inputsz is the number of reads used to create the Input (control) file. For Blueprint this is unique_reads_post_filter (optional, used for calculating z-score)
  outfile lists a filename for outputting results
  outdir is the path to output directory for output and temporary files
  keep.files=TRUE will not delete temporary count files. Can be 'IP' or 'Input'
  plot=TRUE will draw the CHANCE plot, use plot=file.png to specify filename
  counts=TRUE will treat the files as pre-generated counts in the fourth column of a tab-separated file. Can be 'IP' or 'Input'
  bins bedfile of bin coordinates [default: 1000bpregions.bed]
  path to wiggletools directory if not installed in system path 
  sleep is the system sleep time after generating counts to wait for the temporary file to be written [default:15]\n");
	quit(save = "no", status = 1, runLast = FALSE) 
}

ipfile = args$ip
inputfile = args$input

#Calculate bin read counts from bigWig using wiggletools 

if(args$counts == FALSE | args$counts == "Input")
{
  ipfile = gsub(".*/", "", ipfile)
  ipfile = gsub("\\..*?$", ".1000bp.txt", ipfile)
  ipfile = paste(args$outdir, "/", ipfile, sep="")

  system(paste(args$path,"wiggletools apply_paste ", ipfile,"  meanI ", args$bins, " ", args$ip, sep=""))
  Sys.sleep(args$sleep)
}

if(args$counts == FALSE | args$counts == "IP")
{
  inputfile = gsub(".*/", "", inputfile)
  inputfile = gsub("\\..*?$", ".1000bp.txt", inputfile)  
  inputfile = paste(args$outdir, "/", inputfile, sep="")

  system(paste(args$path,"wiggletools apply_paste ", inputfile,"  meanI ", args$bins, " ", args$input, sep=""))
  Sys.sleep(args$sleep)
}





   y1 = as.integer(args$ipsz)
   y2 = as.integer(args$inputsz)

   x  = cbind(read.delim(ipfile, head=F, colClasses=c(rep("NULL",3), "numeric"), na.strings="nan"), read.delim(inputfile, head=F, colClasses=c(rep("NULL",3), "numeric"), na.strings="nan"))

   x = x[order(x[,1], na.last=FALSE),]

   x[is.na(x)] <- 0

   n = nrow(x)

   cs1 = cumsum(x[,1])
   cs2 = cumsum(x[,2])
 
   s1 = cs1[n]
   s2 = cs2[n]

   csdiff = cs2/s2 - cs1/s1;

   errors = rep(0,3)
  
   if(args$plot != FALSE)
   {
   if(args$plot == TRUE)
   {

    file1 = gsub(".*/", "", args$ip)
    file2 = gsub(".*/", "", args$input)
 
    png(file = paste(args$outdir,"/",file1,"_vs_",file2,".png", sep=""))
   }
 
   else
   {
    png(file = args$plot)
   }
   plot(y = cs1/s1, x = (1:n)/n, main = "Cumulative percentage enrichment in each channel", ylab = "Percentage of tags", xlab = "Percentage of bins", col = "darkblue", lwd = 3, ty="l")
   lines(y = cs2/s2, x = (1:n)/n, col = "red", lwd = 3)
   }


   if(min(csdiff) < -1e-8) { cat(paste("Input enrichment stronger than IP at bin", which.min(csdiff), "\n")); abline(v = which.min(csdiff)/n, col= "orange", lty = 3, lwd = 2) } 
   if(min(csdiff) < -1e-8) { errors[1] = which.min(csdiff) } 

   k = which.max(csdiff)
   p = cs1[k]/s1
   q = cs2[k]/s2
   if(p == 0) {cat(paste("Zero-enriched IP, maximum difference at bin", k, "\n"))}
   if(p == 0) {errors[2] = k}
   if(args$plot != FALSE)
   { abline(v = k/n, col = "darkgreen", lty = 3, lwd = 2) }

   k_pcr = n - floor(0.01 * n)
   if(cs2[k_pcr]/s2 < 0.75) 
   {
     cat(paste("PCR amplification bias in Input, coverage of 1% of genome", 1 - cs2[k_pcr]/s2, "\n")); 
     if(args$plot != FALSE)
     { abline(h = cs2[k_pcr]/s2, col = "turquoise", lty = 3, lwd = 2) }
   }
   
   if(cs2[k_pcr]/s2 < 0.75) {errors[3] = 1 - cs2[k_pcr]/s2}

   if(args$plot != FALSE) { dev.off() }

   div = -(p + q)/2 * log2((p + q)/2) - (1 - (p + q)/2) * log2(1 - (p + q)/2) + p/2 * log2(p) + (1 - p)/2 * log2(1 - p) + q/2 * log2(q) + (1 - q)/2 * log2(1 - q)
   f = sqrt(div)

   zscore = NA 

   if(y1 > 0 & y2 > 0)
   {
   ##dij = df/dpij, p11 = p, p21 = 1 - p, p12 = q, p22 = 1 - q
   d11 = log2(2 * p / (p + q)) / (4 * f)
   d21 = log2(2 * (1 - p) / (2 - p - q)) / (4 * f)
   d12 = log2(2 * q / (p + q)) / (4 * f)
   d22 = log2(2 * (1 - q) / (2 - p - q)) / (4 * f)

   ##Var(f) = t(âˆ‡f) %*% Î£ %*% âˆ‡f , where Î£ is the variance-covariance matrix for ((p11, p21), (p12, p22))
   varf = p * (1 - p) / y1 * ( d11 * (d11 - d21) + d21 * (d21 - d11) ) + q * (1 - q) / y2 * (d21 * (d21 - d22) + d22 * (d22 - d21) )

   zscore = f/sqrt(varf)
    }

   header = c("p", "q", "divergence", "z_score", "percent_genome_enriched", "input_scaling_factor", "differential_percentage_enrichment") 
   results = round(c(p, q, div, zscore, 100 * (n - k)/n, p / q * y1 / y2, 100 * (q - p)), 4)
   
   if(args$outfile != "")
   {
    sink(paste(args$outdir, "/", args$outfile, sep=""), append=TRUE)
    cat(sprintf("%s	%s	%.5f	%.5f	%.5f	%.2f	%.3f	%.5f	%.4f	%d	%d	%.4f\n", args$ip, args$input, p, q, div, zscore, 100 * (n - k)/n, p / q * y1 / y2, 100 * (q - p), errors[1], errors[2], errors[3]))
   }
 
   names(results) = header
   results

   if((args$keep.files == FALSE | args$keep.files == "Input") & (args$counts == FALSE | args$counts == "Input"))
   {
    system(paste("rm", ipfile))
   }

  if((args$keep.files == FALSE | args$keep.files == "IP") & (args$counts == FALSE | args$counts == "IP"))
   {
    system(paste("rm", inputfile))
   }
