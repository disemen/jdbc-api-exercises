package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProductDaoImpl implements ProductDao {
    private final DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        Objects.requireNonNull(product);

        String insert = "INSERT INTO products (name, producer, price, expiration_date) VALUES (?, ?, ?, ?);";

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(insert, PreparedStatement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, product.getName());
            preparedStatement.setString(2, product.getProducer());
            preparedStatement.setBigDecimal(3, product.getPrice());
            preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
            preparedStatement.executeUpdate();

            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                product.setId(generatedKeys.getLong(1));
            }
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Error saving product: %s", product));
        }
    }

    @Override
    public List<Product> findAll() {
        String findAll = "SELECT * FROM products";

        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(findAll);

            List<Product> product = new ArrayList<>();
            while (resultSet.next()) {
                Product newProduct = new Product();
                newProduct.setId(resultSet.getLong("id"));
                newProduct.setName(resultSet.getString("name"));
                newProduct.setProducer(resultSet.getString("producer"));
                newProduct.setPrice(resultSet.getBigDecimal("price", 0));
                newProduct.setExpirationDate(resultSet.getDate("expiration_date").toLocalDate());

                product.add(newProduct);
            }

            return product;
        } catch (SQLException e) {
            throw new DaoOperationException(e.getMessage());
        }
    }

    @Override
    public Product findOne(Long id) {
        String findOne = "SELECT * FROM products WHERE id = ?";

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(findOne);
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();

            Product product = new Product();
            if (resultSet.next()) {
                product.setId(resultSet.getLong(1));
                product.setName(resultSet.getString(2));
                product.setProducer(resultSet.getString(3));
                product.setPrice(resultSet.getBigDecimal(4));
                product.setExpirationDate(resultSet.getDate(5).toLocalDate());
                product.setCreationTime(resultSet.getTimestamp(6).toLocalDateTime());

                return product;
            } else {
                throw new DaoOperationException(String.format("Product with id = %d does not exist", id));
            }
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Product with id = %d does not exist", id));
        }
    }

    @Override
    public void update(Product product) {
        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }

        String update = "UPDATE products " +
                        "SET name = ?, producer = ?, price = ?, expiration_date = ? " +
                        "WHERE id = ?";

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(update);
            preparedStatement.setString(1, product.getName());
            preparedStatement.setString(2, product.getProducer());
            preparedStatement.setBigDecimal(3, product.getPrice());
            preparedStatement.setDate(4, Date.valueOf(product.getExpirationDate()));
            preparedStatement.setLong(5, product.getId());

            int execute = preparedStatement.executeUpdate();
            if (execute == 0) {
                throw new DaoOperationException(String.format("Product with id = %d does not exist", product.getId()));
            }
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Product with id = %d does not exist", product.getId()));
        }
    }

    @Override
    public void remove(Product product) {
        if (product.getId() == null) {
            throw new DaoOperationException("Cannot find a product without ID");
        }

        String delete = "DELETE FROM products WHERE id = ?";

        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(delete);
            preparedStatement.setLong(1, product.getId());

            int update = preparedStatement.executeUpdate();
            if (update == 0) {
                throw new DaoOperationException(String.format("Product with id = %d does not exist", product.getId()));
            }
        } catch (SQLException e) {
            throw new DaoOperationException(String.format("Product with id = %d does not exist", product.getId()));
        }
    }
}
