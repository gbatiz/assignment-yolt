output "cluster_id" {
  description = "ID of the EMR cluster"
  value       = aws_emr_cluster.cluster.id
}
