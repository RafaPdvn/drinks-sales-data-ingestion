# ####################################################################################################
# # Canais de comunicação
# ####################################################################################################

# locals {
#   emails_alerta_dev = {
#     "Nome da Pessoa 1" = "pessoa@g.globo"
#     "Nome da Pessoa 2" = "pessoa@g.globo"
#   }
#   emails_alerta_prod = {
#     "Nome da Pessoa 1" = "pessoa@g.globo"
#     "Nome da Pessoa 2" = "pessoa@g.globo"
#   }
# }

# resource "google_monitoring_notification_channel" "alertas" {
#   for_each = local.isdev ? local.emails_alerta_dev : local.emails_alerta_prod

#   display_name = each.key
#   type = "email"
#   labels = {
#     email_address = each.value
#   }
#   force_delete = true
# }

# ####################################################################################################
# # Políticas de Alerta
# ####################################################################################################

# resource "google_monitoring_alert_policy" "fila-longa" {
#   display_name = "${var.solucao} - Fila longa${local.sufixo}"
#   combiner = "OR"
#   user_labels = local.labels
#   conditions {
#     display_name = "Oldest unacked message"
#     condition_threshold {
#       filter = <<FILTER
#         metric.type = "pubsub.googleapis.com/subscription/oldest_unacked_message_age"
#         resource.type = "pubsub_subscription"
#         resource.labels.subscription_id = "${google_pubsub_subscription.XXX.name}"
#       FILTER
#       aggregations {
#         alignment_period = "60s"
#         per_series_aligner = "ALIGN_MAX"
#       }
#       comparison = "COMPARISON_GT"
#       threshold_value = 3000 # 50 minutos
#       duration = "300s" # 5 minutos
#     }
#   }
#   notification_channels = [ for email in google_monitoring_notification_channel.alertas: email.name ]
# }
