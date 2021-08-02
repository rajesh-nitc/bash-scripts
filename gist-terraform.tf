//https://github.com/GoogleCloudPlatform/gke-security-scenarios-demo/blob/master/terraform/modules/instance/main.tf

// This provider is used to block the subsequent providers until the instance
// is available.
// local-exec providers may run before the host has fully initialized. However, they
// are run sequentially in the order they were defined.
//
// This provider is used to block the subsequent providers until the instance
// is available.
provisioner "local-exec" {
command = <<EOF
        READY=""
        for i in $(seq 1 18); do
          if gcloud compute ssh ${var.hostname} --command uptime; then
            READY="yes"
            break;
          fi
          echo "Waiting for ${var.hostname} to initialize..."
          sleep 10;
        done
        if [[ -z $READY ]]; then
          echo "${var.hostname} failed to start in time."
          echo "Please verify that the instance starts and then re-run `terraform apply`"
          exit 1
        fi
EOF

}

provisioner "local-exec" {
command = "gcloud compute scp --project ${var.project} --zone ${var.zone} --recurse ${path.module}/manifests ${var.hostname}:"
}
}
