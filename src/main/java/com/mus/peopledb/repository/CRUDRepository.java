package com.mus.peopledb.repository;

import com.mus.peopledb.exception.UnableToSaveException;
import com.mus.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository<T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSaveSql(), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);
            int recordsAffected = ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()) {
                long id = rs.getLong(1);
                entity.setId(id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: " + entity);
        }
        return entity;
    }


    public Optional<T> findById(Long id) {
        T entity = null;

        try {
            PreparedStatement ps = connection.prepareStatement(getFindByIdSql());
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(entity);
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement ps = connection.prepareStatement(getFindAllSql());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return entities;
    }

    public long count() {
        long count = 0;
        try {
            PreparedStatement ps = connection.prepareStatement(getCountSql());
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getDeleteSql());
            ps.setLong(1, entity.getId());
            int affectedRecordCount = ps.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(T... entities) { //use varargs for multiple arguments
        try {
            Statement stmt = connection.createStatement();
            String ids = Arrays.stream(entities).map(T::getId).map(String::valueOf).collect(joining(","));
            int affectedRecordCount = stmt.executeUpdate(getDeleteInSql().replace(":ids", ids));
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement((getUpdateSql()));
            mapForUpdate(entity, ps);
            ps.setLong(5, entity.getId());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract String getUpdateSql();


    protected abstract String getDeleteInSql();

    protected abstract String getDeleteSql();


    protected abstract String getCountSql();

    protected abstract String getFindAllSql();

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    /**
     * @return Return a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */

    protected abstract String getFindByIdSql();

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;

    abstract String getSaveSql();
}
