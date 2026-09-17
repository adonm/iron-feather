{{- define "cachey.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- /* Per-zone Service/Deployment name. Call as include "cachey.zoneName" (list $ $zone). */ -}}
{{- define "cachey.zoneName" -}}
{{- $ctx := index . 0 -}}
{{- $zone := index . 1 -}}
{{- printf "%s-cachey-%s" (include "cachey.fullname" $ctx) $zone -}}
{{- end -}}
