import entities.Book;
import entities.Borrow;
import entities.Card;
import queries.*;
import utils.DBInitializer;
import utils.DatabaseConnector;

import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class LibraryManagementSystemImpl implements LibraryManagementSystem {

    private final DatabaseConnector connector;

    public LibraryManagementSystemImpl(DatabaseConnector connector) {
        this.connector = connector;
    }

    @Override
    public ApiResult storeBook(Book book) {
        Connection conn = connector.getConn();
        try {
            // Check if the book already exists
            PreparedStatement checkStmt = conn.prepareStatement(
                    "SELECT * FROM book WHERE category = ? AND title = ? AND press = ? AND publish_year = ? AND author = ?"
            );
            checkStmt.setString(1, book.getCategory());
            checkStmt.setString(2, book.getTitle());
            checkStmt.setString(3, book.getPress());
            checkStmt.setInt(4, book.getPublishYear());
            checkStmt.setString(5, book.getAuthor());
            ResultSet rs = checkStmt.executeQuery();
            if (rs.next()) {
                // The book already exists
                return new ApiResult(false, "The book already exists in the library.");
            }

            // Insert the new book
            PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO book (category, title, press, publish_year, author, price, stock) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS
            );
            insertStmt.setString(1, book.getCategory());
            insertStmt.setString(2, book.getTitle());
            insertStmt.setString(3, book.getPress());
            insertStmt.setInt(4, book.getPublishYear());
            insertStmt.setString(5, book.getAuthor());
            insertStmt.setDouble(6, book.getPrice());
            insertStmt.setInt(7, book.getStock());
            insertStmt.executeUpdate();

            // Retrieve the generated book_id
            ResultSet generatedKeys = insertStmt.getGeneratedKeys();
            if (generatedKeys.next()) {
                book.setBookId(generatedKeys.getInt(1));
            }

            // The book has been successfully stored
            return new ApiResult(true, "The book has been successfully stored in the library.");
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult incBookStock(int bookId, int deltaStock) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult storeBook(List<Book> books) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult removeBook(int bookId) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult modifyBookInfo(Book book) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult queryBook(BookQueryConditions conditions) {
        Connection conn = connector.getConn();
        try {
            StringBuilder query = new StringBuilder("SELECT * FROM book WHERE 1=1");
            if (conditions.getCategory() != null) {
                query.append(" AND category = ?");
            }
            if (conditions.getTitle() != null) {
                query.append(" AND title LIKE ?");
            }
            if (conditions.getPress() != null) {
                query.append(" AND press LIKE ?");
            }
            if (conditions.getMinPublishYear() != null) {
                query.append(" AND publish_year >= ?");
            }
            if (conditions.getMaxPublishYear() != null) {
                query.append(" AND publish_year <= ?");
            }
            if (conditions.getAuthor() != null) {
                query.append(" AND author LIKE ?");
            }
            if (conditions.getMinPrice() != null) {
                query.append(" AND price >= ?");
            }
            if (conditions.getMaxPrice() != null) {
                query.append(" AND price <= ?");
            }
            PreparedStatement stmt = conn.prepareStatement(query.toString());
            int index = 1;
            if (conditions.getCategory() != null) {
                stmt.setString(index++, conditions.getCategory());
            }
            if (conditions.getTitle() != null) {
                stmt.setString(index++, "%" + conditions.getTitle() + "%");
            }
            if (conditions.getPress() != null) {
                stmt.setString(index++, "%" + conditions.getPress() + "%");
            }
            if (conditions.getMinPublishYear() != null) {
                stmt.setInt(index++, conditions.getMinPublishYear());
            }
            if (conditions.getMaxPublishYear() != null) {
                stmt.setInt(index++, conditions.getMaxPublishYear());
            }
            if (conditions.getAuthor() != null) {
                stmt.setString(index++, "%" + conditions.getAuthor() + "%");
            }
            if (conditions.getMinPrice() != null) {
                stmt.setDouble(index++, conditions.getMinPrice());
            }
            if (conditions.getMaxPrice() != null) {
                stmt.setDouble(index++, conditions.getMaxPrice());
            }
            ResultSet rs = stmt.executeQuery();
            List<Book> books = new ArrayList<>();
            while (rs.next()) {
                Book book = new Book();
                book.setBookId(rs.getInt("book_id"));
                book.setCategory(rs.getString("category"));
                book.setTitle(rs.getString("title"));
                book.setPress(rs.getString("press"));
                book.setPublishYear(rs.getInt("publish_year"));
                book.setAuthor(rs.getString("author"));
                book.setPrice(rs.getDouble("price"));
                book.setStock(rs.getInt("stock"));
                books.add(book);
            }
            Comparator<Book> cmp = conditions.getSortBy().getComparator();
            if (conditions.getSortOrder() == SortOrder.DESC) {
                cmp = cmp.reversed();
            }
            Comparator<Book> comparator = cmp;
            Comparator<Book> sortComparator = (lhs, rhs) -> {
                if (comparator.compare(lhs, rhs) == 0) {
                    return lhs.getBookId() - rhs.getBookId();
                }
                return comparator.compare(lhs, rhs);
            };
            books.sort(sortComparator);

            // Create a new BookQueryResults object and add the books to it
            BookQueryResults results = new BookQueryResults(books);

            // Return the results as the payload of the ApiResult
            return new ApiResult(true, results);
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult borrowBook(Borrow borrow) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult returnBook(Borrow borrow) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult showBorrowHistory(int cardId) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult registerCard(Card card) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult removeCard(int cardId) {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult showCards() {
        return new ApiResult(false, "Unimplemented Function");
    }

    @Override
    public ApiResult resetDatabase() {
        Connection conn = connector.getConn();
        try {
            Statement stmt = conn.createStatement();
            DBInitializer initializer = connector.getConf().getType().getDbInitializer();
            stmt.addBatch(initializer.sqlDropBorrow());
            stmt.addBatch(initializer.sqlDropBook());
            stmt.addBatch(initializer.sqlDropCard());
            stmt.addBatch(initializer.sqlCreateCard());
            stmt.addBatch(initializer.sqlCreateBook());
            stmt.addBatch(initializer.sqlCreateBorrow());
            stmt.executeBatch();
            commit(conn);
        } catch (Exception e) {
            rollback(conn);
            return new ApiResult(false, e.getMessage());
        }
        return new ApiResult(true, null);
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void commit(Connection conn) {
        try {
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
