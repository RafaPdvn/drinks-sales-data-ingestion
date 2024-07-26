resource "google_dataform_repository_release_config" "release" {
    provider = google-beta
    name = "release-${var.solucao}${local.sufixo}"
    project = var.projeto-pipeline
    region = var.regiao
    repository = google_dataform_repository.dataform_respository.name
    git_commitish = var.branch
    cron_schedule = "33 11 * * *"
    time_zone = "America/Sao_Paulo"

    code_compilation_config {
      default_database = var.projeto-pipeline
      default_schema   = local.isdev ? "dataform_${var.branch}" : "dataform"
    }
}