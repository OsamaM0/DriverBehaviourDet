{{/*
Reusable worker Deployment template.
Usage: include "driver-analytics.workerDeployment" (dict "ctx" . "name" "ingest" "values" .Values.ingest "image" "ingest")
*/}}
{{- define "driver-analytics.workerDeployment" -}}
{{- $ctx := .ctx -}}
{{- $name := .name -}}
{{- $values := .values -}}
{{- $image := .image -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "driver-analytics.fullname" $ctx }}-{{ $name }}
  labels:
    {{- include "driver-analytics.commonLabels" $ctx | nindent 4 }}
    component: {{ $name }}
spec:
  replicas: {{ $values.replicaCount }}
  selector:
    matchLabels:
      app.kubernetes.io/instance: {{ $ctx.Release.Name }}
      component: {{ $name }}
  template:
    metadata:
      labels:
        app.kubernetes.io/instance: {{ $ctx.Release.Name }}
        component: {{ $name }}
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9100"
    spec:
      {{- with $values.nodeSelector }}
      nodeSelector: {{ toYaml . | nindent 8 }}
      {{- end }}
      containers:
        - name: {{ $name }}
          image: "{{ $ctx.Values.global.imageRegistry }}/driver-analytics-{{ $image }}:{{ $ctx.Values.global.imageTag }}"
          imagePullPolicy: {{ $ctx.Values.global.imagePullPolicy }}
          envFrom:
            - configMapRef:
                name: {{ include "driver-analytics.fullname" $ctx }}-env
          ports:
            - name: metrics
              containerPort: 9100
          resources: {{ toYaml $values.resources | nindent 12 }}
{{- end -}}
