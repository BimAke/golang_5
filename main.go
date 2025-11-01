package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type User struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	City        string `json:"city"`
	TotalOrders int    `json:"total_orders"`
}

func main() {
	conn, err := pgxpool.New(context.Background(), "postgres://postgres:postgres@localhost:5432/mydb")
	if err != nil {
		log.Fatal(err)
	}

	app := fiber.New()

	app.Get("/users", func(c *fiber.Ctx) error {
		city := c.Query("city", "")
		limit, _ := strconv.Atoi(c.Query("limit", "10"))
		offset, _ := strconv.Atoi(c.Query("offset", "0"))

		start := time.Now()
		users, err := fetchUsers(c.Context(), conn, city, limit, offset)
		queryTime := time.Since(start)

		c.Set("X-Query-Time", queryTime.String())

		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(users)
	})

	app.Listen(":3000")
}

func fetchUsers(ctx context.Context, db *pgxpool.Pool, city string, limit, offset int) ([]User, error) {
	params := []interface{}{}
	where := ""

	if city != "" {
		params = append(params, city)
		where = fmt.Sprintf("WHERE u.city = $%d", len(params))
	}

	params = append(params, limit)
	limitIdx := len(params)

	params = append(params, offset)
	offsetIdx := len(params)

	query := fmt.Sprintf(`
		SELECT u.id, u.name, u.city,
		COUNT(o.id) AS total_orders
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		%s
		GROUP BY u.id
		ORDER BY total_orders DESC, u.id DESC
		LIMIT $%d OFFSET $%d
	`, where, limitIdx, offsetIdx)

	rows, err := db.Query(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []User
	for rows.Next() {
		va
