####################################################################################################
# Transfere o código do dataproc para o bucket de codigos
####################################################################################################

resource "google_storage_bucket_object" "dataproc-code" {
  for_each = fileset("../codigo/dataproc", "**/*")

  name = "dataproc/${each.value}"
  bucket = google_storage_bucket.codigos.name
  source = "../codigo/dataproc/${each.value}"
}

resource "google_storage_bucket_object" "bootstrap" {
  name = "dataproc/bootstrap.sh"
  bucket = google_storage_bucket.codigos.name
  content = templatefile("bootstrap.template", {
    bucket = google_storage_bucket.codigos.name
  })
}

####################################################################################################
# Dataprocs
####################################################################################################

# Variaveis base de todos os steps do dataproc
locals {
  args = [
    "DELTA_DAY", #1
    "DATA_INICIO", #2
    "DATA_FIM", #3
    "RUN_LIST", #4
    var.env, #5
    var.projeto-pipeline, #6
    var.solucao #7
  ]
}

# Dataproc
# resource "google_dataproc_workflow_template" "dataproc" {
#   name = "${var.solucao}${local.sufixo}"
#   dag_timeout = "${60 * 60 * 24}s" # 24 horas
#   location = var.regiao
#   labels = local.labels
#   placement {
#     managed_cluster {
#       cluster_name = "${var.solucao}${local.sufixo}"
#       config {
#         staging_bucket = google_storage_bucket.tmp-dataproc.name
#         temp_bucket = google_storage_bucket.tmp-dataproc.name
#         initialization_actions {
#           executable_file = "gs://${google_storage_bucket.codigos.name}/dataproc/bootstrap.sh"
#         }
#         endpoint_config {
#           enable_http_port_access = true
#         }
#         software_config {
#           image_version = "2.0.45-debian10"
#           properties = {
#             "spark:spark.jars.ivySettings" = "/tmp/ivy-settings.xml"
#             "spark:spark.jars.repositories" = "https://artifactory.globoi.com/"
#             "spark:spark.jars" = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar"
#             "spark:spark.sql.sources.partitionOverwriteMode" = "dynamic"
#             "spark:spark.dynamicAllocation.enabled" = "true"
#           }
#         }
#         gce_cluster_config {
#           zone = ""
#           internal_ip_only = true
#           service_account_scopes = [
#             "https://www.googleapis.com/auth/cloud-platform"
#           ]
#           subnetwork = var.subnet
#         }
#         master_config {
#           num_instances = 1
#           machine_type = "n1-standard-2"
#           disk_config {
#             boot_disk_type = "pd-standard"
#             boot_disk_size_gb = 50
#           }
#         }
#         worker_config {
#           num_instances = 2
#           machine_type = "n1-standard-2"
#           disk_config {
#             boot_disk_type = "pd-standard"
#             boot_disk_size_gb = 50
#           }
#         }
#       }
#     }
#   }
#   jobs {
#     step_id = "load"
#     pyspark_job {
#       main_python_file_uri = "gs://${google_storage_bucket.codigos.name}/dataproc/load/main.py"
#       args = concat(local.args, [
#         google_storage_bucket.landing.name, #8 bucket landing - leitura
#         google_bigquery_table.raw.table_id, #9 Tabela raw - escrita
#       ])
#     }
#   }
#   parameters {
#     name = "DELTA_DAY"
#     fields = [
#       "jobs['load'].pysparkJob.args[0]",
#     ]
#   }
#   parameters {
#     name = "DATA_INICIO"
#     fields = [
#       "jobs['load'].pysparkJob.args[1]",
#     ]
#   }
#   parameters {
#     name = "DATA_FIM"
#     fields = [
#       "jobs['load'].pysparkJob.args[2]",
#     ]
#   }
#   parameters {
#     name = "RUN_LIST"
#     fields = [
#       "jobs['load'].pysparkJob.args[3]",
#     ]
#   }
# }
