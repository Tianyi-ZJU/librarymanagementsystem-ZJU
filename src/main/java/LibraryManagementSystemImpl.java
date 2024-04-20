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

    public ApiResult incBookStock(int bookId, int deltaStock) {
        Connection conn = connector.getConn();
        try {
            // Get the book from the database
            PreparedStatement stmt = conn.prepareStatement("SELECT * FROM book WHERE book_id = ?");
            stmt.setInt(1, bookId);
            ResultSet rs = stmt.executeQuery();

            if (!rs.next()) {
                // The book does not exist
                return new ApiResult(false, "Book does not exist");
            }

            int currentStock = rs.getInt("stock");

            // Check if the final stock will be a non-negative number
            if (currentStock + deltaStock < 0) {
                return new ApiResult(false, "The final stock cannot be a negative number");
            }

            // Update the stock of the book
            stmt = conn.prepareStatement("UPDATE book SET stock = stock + ? WHERE book_id = ?");
            stmt.setInt(1, deltaStock);
            stmt.setInt(2, bookId);
            stmt.executeUpdate();

            return new ApiResult(true, "The stock has been updated successfully");
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
    }
    @Override
    public ApiResult storeBook(List<Book> books) {
        Connection conn = connector.getConn();
        try {
            // Start a transaction
            conn.setAutoCommit(false);

            // Prepare the SQL statement
            PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO book (category, title, press, publish_year, author, price, stock) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS
            );

            for (Book book : books) {
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
                    // The book already exists, rollback the transaction
                    conn.rollback();
                    return new ApiResult(false, "The book already exists in the library.");
                }

                // Insert the new book
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
            }

            // All books have been successfully stored, commit the transaction
            conn.commit();

            return new ApiResult(true, "All books have been successfully stored in the library.");
        } catch (SQLException e) {
            // An error occurred, rollback the transaction
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public ApiResult removeBook(int bookId) {
        Connection conn = connector.getConn();
        try {
            // Check if the book is currently borrowed
            PreparedStatement checkStmt = conn.prepareStatement(
                    "SELECT * FROM borrow WHERE book_id = ? AND return_time = 0"
            );
            checkStmt.setInt(1, bookId);
            ResultSet rs = checkStmt.executeQuery();
            if (rs.next()) {
                // The book is currently borrowed and not returned yet
                return new ApiResult(false, "The book is currently borrowed and not returned yet.");
            }

            // Delete the book from the database
            PreparedStatement deleteStmt = conn.prepareStatement(
                    "DELETE FROM book WHERE book_id = ?"
            );
            deleteStmt.setInt(1, bookId);
            int affectedRows = deleteStmt.executeUpdate();

            if (affectedRows == 0) {
                // The book does not exist
                return new ApiResult(false, "The book does not exist.");
            }

            // The book has been successfully removed
            return new ApiResult(true, "The book has been successfully removed.");
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
    }
    @Override
    public ApiResult modifyBookInfo(Book book) {
        Connection conn = connector.getConn();
        try {
            // Check if the book exists
            PreparedStatement checkStmt = conn.prepareStatement(
                    "SELECT * FROM book WHERE book_id = ?"
            );
            checkStmt.setInt(1, book.getBookId());
            ResultSet rs = checkStmt.executeQuery();
            if (!rs.next()) {
                // The book does not exist
                return new ApiResult(false, "The book does not exist.");
            }

            // Update the book information
            PreparedStatement updateStmt = conn.prepareStatement(
                    "UPDATE book SET category = ?, title = ?, press = ?, publish_year = ?, author = ?, price = ? WHERE book_id = ?"
            );
            updateStmt.setString(1, book.getCategory());
            updateStmt.setString(2, book.getTitle());
            updateStmt.setString(3, book.getPress());
            updateStmt.setInt(4, book.getPublishYear());
            updateStmt.setString(5, book.getAuthor());
            updateStmt.setDouble(6, book.getPrice());
            updateStmt.setInt(7, book.getBookId());
            updateStmt.executeUpdate();

            // The book information has been successfully updated
            return new ApiResult(true, "The book information has been successfully updated.");
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
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
        Connection conn = connector.getConn();
        try {
            // Check if the book exists
            PreparedStatement bookCheckStmt = conn.prepareStatement(
                    "SELECT * FROM book WHERE book_id = ?"
            );
            bookCheckStmt.setInt(1, borrow.getBookId());
            ResultSet bookCheckRs = bookCheckStmt.executeQuery();
            if (!bookCheckRs.next()) {
                // The book does not exist
                return new ApiResult(false, "The book does not exist.");
            }

            // Check if the card exists
            PreparedStatement cardCheckStmt = conn.prepareStatement(
                    "SELECT * FROM card WHERE card_id = ?"
            );
            cardCheckStmt.setInt(1, borrow.getCardId());
            ResultSet cardCheckRs = cardCheckStmt.executeQuery();
            if (!cardCheckRs.next()) {
                // The card does not exist
                return new ApiResult(false, "The card does not exist.");
            }

            // Check if the user has already borrowed the book but not returned it
            PreparedStatement checkStmt = conn.prepareStatement(
                    "SELECT * FROM borrow WHERE book_id = ? AND card_id = ? AND return_time = 0"
            );
            checkStmt.setInt(1, borrow.getBookId());
            checkStmt.setInt(2, borrow.getCardId());
            ResultSet rs = checkStmt.executeQuery();
            if (rs.next()) {
                // The user has already borrowed the book but not returned it
                return new ApiResult(false, "The user has already borrowed the book but not returned it.");
            }

            // Check if the book is in stock
            PreparedStatement stockStmt = conn.prepareStatement(
                    "SELECT stock FROM book WHERE book_id = ?"
            );
            stockStmt.setInt(1, borrow.getBookId());
            ResultSet stockRs = stockStmt.executeQuery();
            if (stockRs.next() && stockRs.getInt("stock") <= 0) {
                // The book is out of stock
                return new ApiResult(false, "The book is out of stock.");
            }

            // Insert the new borrow record
            PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO borrow (book_id, card_id, borrow_time) VALUES (?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS
            );
            insertStmt.setInt(1, borrow.getBookId());
            insertStmt.setInt(2, borrow.getCardId());
            insertStmt.setLong(3, borrow.getBorrowTime());
            insertStmt.executeUpdate();

            // Update the stock of the book
            PreparedStatement updateStmt = conn.prepareStatement(
                    "UPDATE book SET stock = stock - 1 WHERE book_id = ?"
            );
            updateStmt.setInt(1, borrow.getBookId());
            updateStmt.executeUpdate();

            // The book has been successfully borrowed
            return new ApiResult(true, "The book has been successfully borrowed.");
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
    }
    @Override
    public ApiResult returnBook(Borrow borrow) {
        Connection conn = connector.getConn();
        try {
            // Check if the borrow record exists and the book has not been returned yet
            PreparedStatement checkStmt = conn.prepareStatement(
                    "SELECT * FROM borrow WHERE book_id = ? AND card_id = ? AND return_time = 0"
            );
            checkStmt.setInt(1, borrow.getBookId());
            checkStmt.setInt(2, borrow.getCardId());
            ResultSet rs = checkStmt.executeQuery();
            if (!rs.next()) {
                // The borrow record does not exist or the book has already been returned
                return new ApiResult(false, "The borrow record does not exist or the book has already been returned.");
            }

            // Check if return_time is greater than borrow_time
            long borrowTime = rs.getLong("borrow_time");
            if (borrow.getReturnTime() <= borrowTime) {
                // return_time is not greater than borrow_time
                return new ApiResult(false, "Return time must be greater than borrow time.");
            }

            // Update the return time of the borrow record
            PreparedStatement updateStmt = conn.prepareStatement(
                    "UPDATE borrow SET return_time = ? WHERE book_id = ? AND card_id = ? AND return_time = 0"
            );
            updateStmt.setLong(1, borrow.getReturnTime());
            updateStmt.setInt(2, borrow.getBookId());
            updateStmt.setInt(3, borrow.getCardId());
            updateStmt.executeUpdate();

            // Update the stock of the book
            PreparedStatement stockStmt = conn.prepareStatement(
                    "UPDATE book SET stock = stock + 1 WHERE book_id = ?"
            );
            stockStmt.setInt(1, borrow.getBookId());
            stockStmt.executeUpdate();

            // The book has been successfully returned
            return new ApiResult(true, "The book has been successfully returned.");
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
    }
    @Override
    public ApiResult showBorrowHistory(int cardId) {
        Connection conn = connector.getConn();
        try {
            // Prepare the SQL statement
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT * FROM borrow WHERE card_id = ? ORDER BY borrow_time DESC, book_id ASC"
            );
            stmt.setInt(1, cardId);

            // Execute the query
            ResultSet rs = stmt.executeQuery();

            // Collect the borrow records
            List<BorrowHistories.Item> items = new ArrayList<>();
            while (rs.next()) {
                // Get the book details
                PreparedStatement bookStmt = conn.prepareStatement(
                        "SELECT * FROM book WHERE book_id = ?"
                );
                bookStmt.setInt(1, rs.getInt("book_id"));
                ResultSet bookRs = bookStmt.executeQuery();
                if (!bookRs.next()) {
                    // The book does not exist
                    return new ApiResult(false, "The book does not exist.");
                }
                Book book = new Book();
                book.setBookId(bookRs.getInt("book_id"));
                book.setCategory(bookRs.getString("category"));
                book.setTitle(bookRs.getString("title"));
                book.setPress(bookRs.getString("press"));
                book.setPublishYear(bookRs.getInt("publish_year"));
                book.setAuthor(bookRs.getString("author"));
                book.setPrice(bookRs.getDouble("price"));
                book.setStock(bookRs.getInt("stock"));

                // Create a new BorrowHistories.Item object
                Borrow borrow = new Borrow();
                borrow.setBookId(rs.getInt("book_id"));
                borrow.setCardId(rs.getInt("card_id"));
                borrow.setBorrowTime(rs.getLong("borrow_time"));
                borrow.setReturnTime(rs.getLong("return_time"));
                BorrowHistories.Item item = new BorrowHistories.Item(cardId, book, borrow);
                items.add(item);
            }

            // Create a new BorrowHistories object
            BorrowHistories histories = new BorrowHistories(items);

            // Return the BorrowHistories object as the payload of the ApiResult
            return new ApiResult(true, histories);
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
    }
    @Override
    public ApiResult registerCard(Card card) {
        Connection conn = connector.getConn();
        try {
            // Check if the card already exists
            PreparedStatement checkStmt = conn.prepareStatement(
                    "SELECT * FROM card WHERE name = ? AND department = ? AND type = ?"
            );
            checkStmt.setString(1, card.getName());
            checkStmt.setString(2, card.getDepartment());
            checkStmt.setString(3, card.getType().getStr());
            ResultSet rs = checkStmt.executeQuery();
            if (rs.next()) {
                // The card already exists
                return new ApiResult(false, "The card already exists.");
            }

            // Insert the new card
            PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO card (name, department, type) VALUES (?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS
            );
            insertStmt.setString(1, card.getName());
            insertStmt.setString(2, card.getDepartment());
            insertStmt.setString(3, card.getType().getStr());
            insertStmt.executeUpdate();

            // Retrieve the generated card_id
            ResultSet generatedKeys = insertStmt.getGeneratedKeys();
            if (generatedKeys.next()) {
                card.setCardId(generatedKeys.getInt(1));
            }

            // The card has been successfully registered
            return new ApiResult(true, "The card has been successfully registered.");
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult removeCard(int cardId) {
        Connection conn = connector.getConn();
        try {
            // Check if the card is currently used to borrow a book
            PreparedStatement checkStmt = conn.prepareStatement(
                    "SELECT * FROM borrow WHERE card_id = ? AND return_time = 0"
            );
            checkStmt.setInt(1, cardId);
            ResultSet rs = checkStmt.executeQuery();
            if (rs.next()) {
                // The card is currently used to borrow a book and the book is not returned yet
                return new ApiResult(false, "The card is currently used to borrow a book and the book is not returned yet.");
            }

            // Delete the card from the database
            PreparedStatement deleteStmt = conn.prepareStatement(
                    "DELETE FROM card WHERE card_id = ?"
            );
            deleteStmt.setInt(1, cardId);
            int affectedRows = deleteStmt.executeUpdate();

            if (affectedRows == 0) {
                // The card does not exist
                return new ApiResult(false, "The card does not exist.");
            }

            // The card has been successfully removed
            return new ApiResult(true, "The card has been successfully removed.");
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
    }

    @Override
    public ApiResult showCards() {
        Connection conn = connector.getConn();
        try {
            // Prepare the SQL statement
            PreparedStatement stmt = conn.prepareStatement(
                    "SELECT * FROM card ORDER BY card_id ASC"
            );

            // Execute the query
            ResultSet rs = stmt.executeQuery();

            // Collect the cards
            List<Card> cards = new ArrayList<>();
            while (rs.next()) {
                Card card = new Card();
                card.setCardId(rs.getInt("card_id"));
                card.setName(rs.getString("name"));
                card.setDepartment(rs.getString("department"));
                card.setType(Card.CardType.values(rs.getString("type")));
                cards.add(card);
            }

            // Create a new CardList object
            CardList cardList = new CardList(cards);

            // Return the CardList object as the payload of the ApiResult
            return new ApiResult(true, cardList);
        } catch (SQLException e) {
            e.printStackTrace();
            return new ApiResult(false, e.getMessage());
        }
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
