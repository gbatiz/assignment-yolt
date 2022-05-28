resource "aws_s3_bucket" "bucket" {
  bucket        = "gbatiz-yolt-assignment"
  force_destroy = true
}

resource "aws_s3_bucket_acl" "bucket_acl" {
  acl    = "private"
  bucket = aws_s3_bucket.bucket.bucket
}

# resource "aws_s3_bucket_object" "raw_data" {
#   bucket     = aws_s3_bucket.bucket.id
#   key        = "data/raw/sample.snappy.parquet"
#   source     = "../data/sample.snappy.parquet"
#   # depends_on = ["aws_s3_bucket.bucket"]
# }

resource "aws_s3_object" "raw_data" {
  for_each = fileset("../data/", "**")
  bucket   = aws_s3_bucket.bucket.id
  key      = "../data/raw/${each.value}"
  source   = "../data/${each.value}"
}

resource "aws_s3_object" "scripts" {
  bucket = aws_s3_bucket.bucket.id
  key    = "scripts/health_violations.py"
  source = "../scripts/health_violations.py"
  # depends_on = ["aws_s3_bucket.bucket"]
}
