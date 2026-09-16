{{- define "ifeather.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ifeather.storageClass" -}}
{{- .Values.persistence.storageClass | default (printf "%s-lake-az-a" (include "ifeather.fullname" .)) -}}
{{- end -}}
