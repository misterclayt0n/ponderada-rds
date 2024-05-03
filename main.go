package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := "mister:misterclayton@tcp(mister.cvs8ddhi6hd2.us-east-1.rds.amazonaws.com)/?parseTime=true"

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Error opening database: %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS mister_db")
	if err != nil {
		log.Fatalf("Error creating database: %s\n", err)
	}

	_, err = db.Exec("USE mister_db")
	if err != nil {
		log.Fatalf("Error selecting database: %s\n", err)
	}

	createTables := []string{
		`CREATE TABLE IF NOT EXISTS pacientes (
			id_paciente INT AUTO_INCREMENT PRIMARY KEY,
			nome VARCHAR(255),
			endereco VARCHAR(255),
			contato VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS veiculos (
			id_veiculo INT AUTO_INCREMENT PRIMARY KEY,
			tipo VARCHAR(255),
			capacidade INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS unidades_saude (
			id_unidade_saude INT AUTO_INCREMENT PRIMARY KEY,
			nome VARCHAR(255),
			endereco VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS rotas (
			id_rota INT AUTO_INCREMENT PRIMARY KEY,
			descricao TEXT,
			distancia DECIMAL(10,2),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS produtos_medicos (
			id_produto INT AUTO_INCREMENT PRIMARY KEY,
			nome VARCHAR(255),
			tipo VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS transportes (
			id_transporte INT AUTO_INCREMENT PRIMARY KEY,
			data TIMESTAMP,
			id_veiculo INT,
			id_paciente INT,
			id_rota INT,
			id_unidade_saude INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (id_veiculo) REFERENCES veiculos(id_veiculo),
			FOREIGN KEY (id_paciente) REFERENCES pacientes(id_paciente),
			FOREIGN KEY (id_rota) REFERENCES rotas(id_rota),
			FOREIGN KEY (id_unidade_saude) REFERENCES unidades_saude(id_unidade_saude)
		);`,
		`CREATE TABLE IF NOT EXISTS entregas (
			id_entrega INT AUTO_INCREMENT PRIMARY KEY,
			id_transporte INT,
			id_produto INT,
			quantidade INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (id_transporte) REFERENCES transportes(id_transporte),
			FOREIGN KEY (id_produto) REFERENCES produtos_medicos(id_produto)
		);`,
	}

	for _, cmd := range createTables {
		_, err = db.Exec(cmd)
		if err != nil {
			log.Fatalf("Error creating tables: %s\n", err)
		}
	}

	_, err = db.Exec(`INSERT INTO pacientes (nome, endereco, contato) VALUES
		('John Doe', '123 Elm St', '123-456-7890'),
		('Jane Smith', '456 Oak St', '987-654-3210');`)
	if err != nil {
		log.Fatalf("Error inserting into pacientes: %s\n", err)
	}

	_, err = db.Exec(`INSERT INTO veiculos (tipo, capacidade) VALUES
		('Car', 4),
		('Bus', 20);`)
	if err != nil {
		log.Fatalf("Error inserting into veiculos: %s\n", err)
	}

	dates := []time.Time{
		time.Date(2023, 1, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 2, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 3, 20, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 4, 25, 0, 0, 0, 0, time.UTC),
	}

	for _, date := range dates {
		for _, idPaciente := range []int{1, 2, 3, 4} {
			for _, idVeiculo := range []int{1, 2, 3, 4} {
				_, err = db.Exec(`INSERT INTO transportes (data, id_veiculo, id_paciente) VALUES (?, ?, ?);`, date, idVeiculo, idPaciente)
				if err != nil {
					log.Fatalf("Error inserting into transportes: %s\n", err)
				}
			}
		}
	}

	query := `
	SELECT id_veiculo, YEAR(data) AS year, MONTH(data) AS month, COUNT(DISTINCT id_paciente) AS total_pacientes
	FROM transportes
	GROUP BY id_veiculo, YEAR(data), MONTH(data);
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Error executing query: %s\n", err)
	}
	defer rows.Close()

	log.Println("ID Veículo | Ano | Mês | Total Pacientes")
	for rows.Next() {
		var idVeiculo, year, month, totalPacientes int
		if err := rows.Scan(&idVeiculo, &year, &month, &totalPacientes); err != nil {
			log.Fatal(err)
		}
		log.Printf("%9d | %4d | %4d | %15d\n", idVeiculo, year, month, totalPacientes)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	log.Println("Query executed and data processed successfully!")
}
