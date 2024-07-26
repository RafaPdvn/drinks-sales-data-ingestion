resource "google_dataform_repository_workflow_config" "config" {
     
    provider = google-beta
    cron_schedule                      = "35 11 * * * "
    name                               = "workflow-${var.solucao}${local.sufixo}"
    project                            = var.projeto-pipeline
    region                             = var.regiao
    release_config                     = "projects/${var.projeto-pipeline}/locations/${var.regiao}/repositories/${google_dataform_repository.dataform_respository.name}/releaseConfigs/${google_dataform_repository_release_config.release.name}"
    repository                         = google_dataform_repository.dataform_respository.name
    time_zone                          = "America/Sao_Paulo"


    invocation_config {
        fully_refresh_incremental_tables_enabled = false
        included_tags                            = []
        transitive_dependencies_included         = true
        transitive_dependents_included           = false

        included_targets {
            database = var.projeto-pipeline
            name     = "tb_vinculo_uf"
            schema   = local.isdev ? "dataform_${var.branch}" : "dataform"
        }

        included_targets {
            database = var.projeto-pipeline
            name     = "tb_receita"
            schema   = local.isdev ? "dataform_${var.branch}" : "dataform"
        }
    }
    timeouts {}
}