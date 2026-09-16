{{- define "zerofs.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "zerofs.gwName" -}}
{{- $ctx := index . 0 -}}
{{- $zone := index . 1 -}}
{{- printf "%s-gw-%s" (include "zerofs.fullname" $ctx) $zone -}}
{{- end -}}

{{- define "zerofs.isWriter" -}}
{{- $ctx := index . 0 -}}
{{- $zone := index . 1 -}}
{{- if eq $zone $ctx.Values.writerZone }}writer{{ else }}cache{{ end -}}
{{- end -}}
