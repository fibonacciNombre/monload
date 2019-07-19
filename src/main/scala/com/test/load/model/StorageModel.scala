package com.test.load.model

trait StorageModel {
  
}

case class PreCustomer(
    id: String,
    profile: String
)

case class FileCustomer(
    id: String,
    nombre: String,
    apellido: String,
    correo: String,
    active: Boolean
)