package com.mus.peopledb.repository;

import com.mus.peopledb.model.Person;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTest {
    private Connection connection;
    private PeopleRepository repo;

    @BeforeEach
    void setUp() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/people", "root", "");
        connection.setAutoCommit(false);
        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() throws SQLException {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("+0")));
        Person savedPerson = repo.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("+0")));
        Person bobby = new Person("Bobby", "Matt", ZonedDateTime.of(1990, 12, 1, 15, 15, 0, 0, ZoneId.of("+0")));
        Person savedPerson1 = repo.save(john);
        Person savedPerson2 = repo.save(bobby);
        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canFindPersonById() {
        Person savedPerson = repo.save(new Person("Jarret", "jackson", ZonedDateTime.of(1990, 12, 12, 10, 15, 0, 0, ZoneId.of("+0"))));
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = repo.findById(-1L);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canGetCount() {
        long startCount = repo.count();
        repo.save(new Person("Jarret", "Jackson", ZonedDateTime.of(1990, 12, 12, 10, 15, 0, 0, ZoneId.of("+0"))));
        repo.save(new Person("Marta", "Webb", ZonedDateTime.of(1989, 1, 12, 14, 25, 0, 0, ZoneId.of("+0"))));
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete() {
        Person savedPerson = repo.save(new Person("Will", "Dubose", ZonedDateTime.of(1990, 12, 12, 10, 15, 0, 0, ZoneId.of("+0"))));
        long startCount = repo.count();
        repo.delete(savedPerson);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person p1 = repo.save(new Person("Jeremy", "Finch", ZonedDateTime.of(1990, 12, 12, 10, 15, 0, 0, ZoneId.of("+0"))));
        Person p2 = repo.save(new Person("Janet", "Fox", ZonedDateTime.of(1990, 12, 12, 10, 15, 0, 0, ZoneId.of("+0"))));
        long startCount = repo.count();
        repo.delete(p1, p2);
        long endCount = repo.count();
        assertThat(endCount).isEqualTo(startCount - 2);
    }

    @Test
    public void canUpdate() {
        Person savedPerson = repo.save(new Person("John", "Smith", ZonedDateTime.of(1990, 12, 12, 10, 15, 0, 0, ZoneId.of("+0"))));

        Person p1 = repo.findById(savedPerson.getId()).get();

        savedPerson.setSalary(new BigDecimal("73000.28"));
        repo.update(savedPerson);

        Person p2 = repo.findById(savedPerson.getId()).get();

        assertThat(p2.getSalary()).isNotEqualTo(p1.getSalary());
    }

    @Test
    public void loadData() throws IOException, SQLException {
        Files.lines(Path.of("C:\\Users\\mikis\\Desktop\\Hr5m_new.csv"))
                .skip(1)
                .limit(21)
                .map(l -> l.split(","))
                .map(a -> {
                    LocalDate dob = LocalDate.parse(a[10], DateTimeFormatter.ofPattern("M/d/yyyy"));
                    dob.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
                    LocalTime tob = LocalTime.parse(a[11], DateTimeFormatter.ofPattern("hh:mm:ss a"));
                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));
                    Person person = new Person(a[2], a[4], zdtob);
                    return person;
                })
                .forEach(repo::save);
//        connection.commit();
    }
}
