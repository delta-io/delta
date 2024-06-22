output "master_node_address" {
  value = data.google_compute_instance.benchmarks_master.network_interface.0.access_config.0.nat_ip
}
