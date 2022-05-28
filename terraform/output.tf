output "cluster_arn" {
  description = "ARN of the EMR cluster"
  value       = aws_emr_cluster.cluster.arn
}
