use std::io::Read;

use super::models::*;
use super::types::*;
use super::DbConnection;
use super::DbResult;
use diesel::deserialize::QueryableByName;
use diesel::sql_query;
use diesel::sql_types;
use diesel::{prelude::*, select};

impl Entry {
    // pub async fn get_all(dbc: &DbConnection) -> DbResult<Vec<Article>> {
    //     let foo = articles.limit(5).load::<Article>(&dbc.get()?);
    //     Ok(foo.expect("Error loading posts"))
    // }

    pub fn save(&self, dbc: &mut DbConnection) -> DbResult<()> {
        use super::schema::entrys::dsl::*;

        // SQLite and MySQL
        diesel::replace_into(entrys)
            .values(self)
            .execute(&mut dbc.get()?)
            .unwrap();

        Ok(())

        // PG, SQLite (and upcoming 2.0.0 Diesel release):
        // diesel::insert_into(entrys)
        //     .values(self)
        //     .on_conflict(id)
        //     .do_update()
        //     .set(self)
        //     .execute(dbc);
        // Ok(())
    }

    pub async fn delete(&self, dbc: &mut DbConnection) -> DbResult<()> {
        use super::schema::entrys::dsl::*;

        diesel::delete(entrys)
            .filter(id.eq(&self.id))
            .execute(&mut dbc.get()?)
            .unwrap();

        Ok(())
    }

    // pub async fn clean_non_existing(
    //     dbc: &DbConnection,
    //     existing_article_files: &[PathBuf],
    // ) -> DbResult<usize> {
    //     let local_paths = existing_article_files
    //         .iter()
    //         .map(|s| s.to_string_lossy())
    //         .collect::<Vec<_>>();

    //     Ok(diesel::delete(articles)
    //         .filter(local_path.ne_all(local_paths))
    //         .execute(&dbc.get()?)?)
    // }
}

#[derive(QueryableByName)]
struct Res {
    #[sql_type = "sql_types::Integer"]
    id: RequestId,
}

impl Request {
    // pub async fn get_all(dbc: &DbConnection) -> DbResult<Vec<Article>> {
    //     let foo = articles.limit(5).load::<Article>(&dbc.get()?);
    //     Ok(foo.expect("Error loading posts"))
    // }

    pub fn save(&self, dbc: &mut DbConnection) -> DbResult<RequestId> {
        // use super::schema::requests::dsl::*;
        // use diesel::result::DatabaseErrorKind::*;
        // use diesel::result::Error::DatabaseError;

        let values = sql_query(
            "
            INSERT INTO 
            Requests(method, url, status_code) 
            VALUES(?, ?, ?) 
            ON CONFLICT DO UPDATE SET id=id RETURNING id
            ",
        )
        .bind::<sql_types::Text, _>(self.method.clone())
        .bind::<sql_types::Text, _>(self.url.clone())
        .bind::<sql_types::Integer, _>(self.status_code)
        .get_results::<Res>(&mut dbc.get()?)?;

        Ok(values.into_iter().nth(0).unwrap().id)

        // }

        // PG, SQLite (and upcoming 2.0.0 Diesel release):
        // diesel::insert_into(entrys)
        //     .values(self)
        //     .on_conflict(id)
        //     .do_update()
        //     .set(self)
        //     .execute(dbc);
        // Ok(())
    }

    // pub async fn delete(&self, dbc: &DbConnection) -> DbResult<()> {
    //     diesel::delete(entrys)
    //         .filter(id.eq(&self.id))
    //         .execute(&dbc.get()?)
    //         .unwrap();

    //     Ok(())
    // }
}

/*

#[cfg(test)]
mod test {
    use crate::db::DbConnection;

    use super::super::ArticleId;
    use super::Article;

    async fn create_test_articles(dbc: &DbConnection) {
        let test1 = Article {
            html: "".into(),
            id: ArticleId::new(),
            local_path: "./examples/post01.md".into(),
            modified: chrono::Local::now().naive_utc(),
            modified_on_disk: chrono::Local::now().naive_utc(),
            published: chrono::Local::now().naive_utc(),
            server_path: "/examples/post01/".into(),
            title: "Example post 01".into(),
        };
        let test2 = Article {
            html: "".into(),
            id: ArticleId::new(),
            local_path: "./examples/post02.md".into(),
            modified: chrono::Local::now().naive_utc(),
            modified_on_disk: chrono::Local::now().naive_utc(),
            published: chrono::Local::now().naive_utc(),
            server_path: "/examples/post02/".into(),
            title: "Example post 02".into(),
        };
        let test3 = Article {
            html: "".into(),
            id: ArticleId::new(),
            local_path: "./examples/non-existing.md".into(),
            modified: chrono::Local::now().naive_utc(),
            modified_on_disk: chrono::Local::now().naive_utc(),
            published: chrono::Local::now().naive_utc(),
            server_path: "/examples/non-existing/".into(),
            title: "Example non existing".into(),
        };

        let _ = test1.save(&dbc).await;
        let _ = test2.save(&dbc).await;
        let _ = test3.save(&dbc).await;
    }

    #[async_std::test]
    async fn test_clean_non_existing() {
        let dbc = DbConnection::new_from_url(":memory:").await.unwrap();
        create_test_articles(&dbc).await;

        assert_eq!(Article::get_all(&dbc).await.unwrap().len(), 3);

        Article::clean_non_existing(
            &dbc,
            &["./examples/post01.md".into(), "./examples/post02.md".into()],
        )
        .await
        .unwrap();

        assert_eq!(Article::get_all(&dbc).await.unwrap().len(), 2);
    }
}

 */
