{{- define "rsocket-simulation.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rsocket-simulation.fullname" -}}
{{- if .Values.fullnameOverride }}{{ .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}{{ printf "%s-%s" .Release.Name (include "rsocket-simulation.name" .) | trunc 63 | trimSuffix "-" }}{{- end }}
{{- end -}}

{{- define "rsocket-simulation.labels" -}}
app.kubernetes.io/name: {{ include "rsocket-simulation.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
