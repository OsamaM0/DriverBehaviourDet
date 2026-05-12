{{- define "driver-analytics.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "driver-analytics.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "driver-analytics.image" -}}
{{- printf "%s/%s:%s" .Values.global.imageRegistry .name (.Values.global.imageTag | default "latest") -}}
{{- end -}}

{{- define "driver-analytics.envFrom" -}}
- configMapRef:
    name: {{ include "driver-analytics.fullname" . }}-env
{{- end -}}

{{- define "driver-analytics.commonLabels" -}}
app.kubernetes.io/name: {{ include "driver-analytics.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
