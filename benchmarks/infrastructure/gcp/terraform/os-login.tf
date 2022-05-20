data "google_client_openid_userinfo" "me" {
}

resource "google_os_login_ssh_public_key" "key_to_login_to_master_node" {
  user = data.google_client_openid_userinfo.me.email
  key  = file(var.public_key_path)
}
