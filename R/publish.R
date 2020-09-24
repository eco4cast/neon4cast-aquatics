
#remotes::install_github("cboettig/prov")
library(contentid)
library(prov)
library(aws.s3)

## Set the following  env vars: 
# AWS_ACCESS_KEY_ID=""
# AWS_SECRET_ACCESS_KEY=""
# AWS_DEFAULT_REGION="data"
# AWS_S3_ENDPOINT="ecoforecast.org"


content_path <- function(path){
  
  vapply(path, 
         function(f){
           hash <- openssl::sha256(file(f, raw = TRUE))
           sub1 <- substr(hash, 1, 2)
           sub2 <- substr(hash, 3, 4)
           file.path(sub1,sub2,hash)
         },
         character(1L), 
         USE.NAMES = FALSE)
}
s3_url <- function(path, 
                   bucket,
                   region = Sys.getenv("AWS_DEFAULT_REGION"), 
                   endpoint = Sys.getenv("AWS_S3_ENDPOINT")){
  paste0(paste0("https://", region, ".", endpoint, "/", bucket, "/"), path)
}



minio_store <-  function(files, 
                         objects = content_path(files),
                         bucket = "content-store",
                         registries = "https://hash-archive.org"){
  
  lapply(seq_along(files), function(i) 
         aws.s3::put_object(files[[i]], objects[[i]], bucket)
  )
  
  urls <- s3_url(objects, bucket = bucket)
  contentid::register(urls, registries)

}


publish <- function(data_in = NULL,
                    code = NULL,
                    data_out = NULL,
                    meta = NULL, 
                    provdb = "prov.json",
                    bucket, 
                    prefix = "",
                    registries = "https://hash-archive.org"){
  
  
  
  ## get the bucket's provenance record first
  suppressMessages({
  unlink("prov.json") # keep records distinct
  if(aws.s3::object_exists(paste0(prefix, provdb), bucket = bucket)){
    aws.s3::save_object(paste0(prefix, provdb), bucket = bucket)
  }
  })
  
  
  prov::write_prov(data_in, code, data_out, meta, 
                   provdb = provdb, append = TRUE)
  
  
  files <- c(data_in,code, data_out, meta)
  
  ## Store to content-based system (version-controlled)
  ids <- minio_store(files, content_path(files), "content-store", registries)
  
  
  ## Store to output and prov to a location-based system
  files <- c(data_out, provdb)
  minio_store(files, paste0(prefix, basename(files)), 
              bucket, "https://hash-archive.org")
  
 
  invisible(ids)
  
}





