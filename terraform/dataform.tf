resource "google_dataform_repository" "dataform_respository" {
  project = var.projeto-pipeline
  provider = google-beta
  name = "${var.solucao}${local.sufixo}"
  region = var.regiao
  timeouts {}

  git_remote_settings {
      url = var.git-repository
      default_branch = var.branch
      authentication_token_secret_version = "projects/${var.projeto-pipeline}/secrets/${var.solucao}/versions/latest"
  }

  workspace_compilation_overrides {
    schema_suffix = var.branch
    default_database = var.projeto-pipeline
  }
}