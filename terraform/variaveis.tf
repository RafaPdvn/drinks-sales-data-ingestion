###################################################################################################
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ATENÇÃO !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#
# Quaisquer alterações feitas nesse arquivo podem causar erros irreparáveis a infraestrutura criada
# pelo terraform. Faça mudanças apenas se você tiver CERTEZA do que está fazendo. Teste as mudanças
# no ambiente de dev quando TIVER que fazê-las. 
###################################################################################################

locals {
  labels = {
    "solucao" = var.solucao
    "branch" = var.branch
  }
  codsolucao = split("-", var.solucao)[0]
  isprod = var.env == "prod"
  isdev = var.env != "prod"
  sufixo = var.env == "prod" ? "" : "-${lower(var.branch)}"
  sufixobq = var.env == "prod" ? "" : "_${lower(var.branch)}"
  topic_dataproc = "projects/${var.projeto-pipeline}/topics/foundation-start-dataproc-workflow"
}

variable "env" {
  type = string
  description = "Ambiente do código"
}

variable hash-projeto {
  type = string
  description = "Hash do nome do projeto"
}

variable "projeto-pipeline" {
  type = string
  description = "Identificador do projeto de pipeline no google"
}

variable "projeto-dados" {
  type = string
  description = "Identificador do projeto de dados no google"
}

variable "regiao" {
  type = string
  description = "Região dos recursos do projeto"
}

variable "branch" {
  type = string
  description = "Branch do código. Usado para a criação da estrutura de códigos a ser usada"
}

variable "solucao" {
  type = string
  description = "Solução do projeto"
}

variable "subnet" {
  type = string
  description = "Subnetwork do projeto"
}

variable "conta-servico" {
  type = string
  description = "Conta de serviço sendo usada"
}

variable "testing" {
  type = bool
  description = "Se essa execução é de um teste"
  default = false
}

variable "vpc" {
  type = string
  description = "VPC utilizada pelo Vertex do projeto"
}

variable "git-repository" {
  type = string
  description = "Repositório da solução no gitlab"
}