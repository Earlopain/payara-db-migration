package net.c5h8no4na.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Singleton;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.sql.DataSource;

import net.c5h8no4na.common.Hash;
import net.c5h8no4na.common.Tuple;
import net.c5h8no4na.common.assertion.Assert;
import net.c5h8no4na.e621.api.E621Client;
import net.c5h8no4na.e621.api.E621Response;
import net.c5h8no4na.e621.api.response.FullUserApi;
import net.c5h8no4na.e621.api.response.PostApi;
import net.c5h8no4na.e621.api.response.TagApi;
import net.c5h8no4na.entity.e621.DestroyedPost;
import net.c5h8no4na.entity.e621.Post;
import net.c5h8no4na.entity.e621.PostFile;
import net.c5h8no4na.entity.e621.Source;
import net.c5h8no4na.entity.e621.Tag;
import net.c5h8no4na.entity.e621.Tag_;
import net.c5h8no4na.entity.e621.User;
import net.c5h8no4na.entity.e621.enums.Extension;
import net.c5h8no4na.entity.e621.enums.Level;
import net.c5h8no4na.entity.e621.enums.Rating;
import net.c5h8no4na.entity.e621.enums.TagType;

@Singleton
public class Backend {
	private static final Logger LOG = Logger.getLogger(Backend.class.getCanonicalName());

	@PersistenceContext(unitName = "e621")
	private EntityManager em;

	@Resource(name = "jdbc/mariadb")
	private DataSource mariaDb;

	private E621Client client = new E621Client("earlopain/test");

	public Post migrateFromMaria(int id) throws SQLException {
		Post p = findOrCreatePost(id);
		if (postIsDestroyed(id) || p != null) {
			deleteFromMariaDb(id);
		}
		return p;
	}

	public Integer mariaDbGetLowestId() throws SQLException {
		try (Connection connection = mariaDb.getConnection();
				PreparedStatement statement = connection.prepareStatement("SELECT id FROM posts ORDER BY id ASC LIMIT 1")) {
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				return rs.getInt(1);
			} else {
				return null;
			}
		} catch (SQLException e) {
			throw e;
		}
	}

	public Post findOrCreatePost(Integer id) {
		if (id == null || postIsDestroyed(id)) {
			return null;
		}
		Post p = em.find(Post.class, id);
		if (p == null) {
			return createPost(id);
		} else {
			return p;
		}
	}

	public Optional<Tuple<byte[], Extension>> getPostFile(Integer id) {
		Post p = em.find(Post.class, id);
		if (p == null) {
			return Optional.empty();
		}
		Optional<PostFile> pf = p.getPostFile();
		if (pf.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(new Tuple<>(pf.get().getFile(), p.getExtension()));
	}

	public Optional<byte[]> getPostFromMariaDb(Integer id) {
		try (Connection connection = mariaDb.getConnection();
				PreparedStatement statement = connection.prepareStatement("SELECT file FROM posts WHERE id = ?")) {
			statement.setInt(1, id);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				byte[] file = rs.getBytes(1);
				if (file == null) {
					// Post is saved, but the file is not
					return Optional.empty();
				} else {
					// Found the file
					return Optional.of(file);
				}
			} else {
				// Post not saved in db
				return Optional.empty();
			}
		} catch (SQLException e) {
			// Some other error
			e.printStackTrace();
			return Optional.empty();
		}
	}

	public void deleteFromMariaDb(int id) throws SQLException {
		try (Connection connection = mariaDb.getConnection();
				PreparedStatement statement = connection.prepareStatement("DELETE FROM posts WHERE id = ?")) {
			statement.setInt(1, id);
			statement.execute();
			LOG.info("Deleted " + id + " from mariadb");
		} catch (SQLException e) {
			throw e;
		}
	}

	private boolean postIsDestroyed(Integer id) {
		return em.find(DestroyedPost.class, id) != null;
	}

	private Post createPost(int id) {
		E621Response<PostApi> response = client.getPost(id);
		if (response.isSuccess()) {
			PostApi post = response.unwrap();
			Post dbPost = new Post();
			dbPost.setId(post.getId());
			dbPost.setCreatedAt(new Timestamp(post.getCreatedAt().getTime()));
			dbPost.setUpdatedAt(post.getUpdatedAt() == null ? null : new Timestamp(post.getUpdatedAt().getTime()));
			dbPost.setWidth(post.getFile().getWidth());
			dbPost.setHeight(post.getFile().getHeight());
			dbPost.setExtension(Extension.from(post.getFile().getExt()).get());
			dbPost.setSize(post.getFile().getSize());
			dbPost.setMd5(post.getFile().getMd5());
			dbPost.setScoreUp(post.getScore().getUp());
			dbPost.setScoreDown(post.getScore().getDown());
			dbPost.setScoreTotal(post.getScore().getTotal());
			dbPost.setTags(findOrCreateTags(post.getTags().getAll()));
			dbPost.setRating(Rating.from(post.getRating()).get());
			dbPost.setFavCount(post.getFavCount());
			dbPost.setDescription(post.getDescription());

			em.persist(dbPost);
			if (post.getApproverId().isPresent()) {
				dbPost.setApprover(findOrCreateUser(post.getApproverId().get()));
			}
			dbPost.setUploader(findOrCreateUser(post.getUploaderId()));
			Assert.notNull(dbPost.getUploader());
			dbPost.setDuration(post.getDuration().orElse(null));

			Optional<byte[]> fileToInsert = getFileFromWhereever(post);
			if (fileToInsert.isPresent()) {
				PostFile pf = new PostFile();
				pf.setPost(dbPost);
				pf.setFile(fileToInsert.get());
				em.persist(pf);
				dbPost.setPostFile(pf);
			}

			// Children
			List<Integer> childIds = post.getRelationships().getChildren();
			List<Post> children = childIds.stream().map(childId -> findOrCreatePost(childId)).collect(Collectors.toList());
			dbPost.setChildren(children);
			for (String source : post.getSources()) {
				Source s = new Source();
				s.setPost(dbPost);
				s.setSource(source);
				em.persist(s);
			}
			return dbPost;
		} else {
			if (response.getResponseCode() == 404) {
				DestroyedPost dp = new DestroyedPost();
				dp.setId(id);
				em.persist(dp);
			}
			return null;
		}
	}

	private Optional<byte[]> getFileFromWhereever(PostApi post) {
		Optional<byte[]> mariadbFile = getPostFromMariaDb(post.getId());
		Optional<byte[]> fileToInsert = Optional.empty();
		if (mariadbFile.isPresent()) {
			String mariaDbMd5 = Hash.getMd5(mariadbFile.get());
			if (post.getFile().getMd5().equals(mariaDbMd5)) {
				fileToInsert = Optional.of(mariadbFile.get());
			}
		}
		if (fileToInsert.isEmpty() && !post.getFlags().isDeleted()) {
			Optional<byte[]> file = client.getFile(post.getFile().getMd5(), post.getFile().getExt());
			fileToInsert = Optional.of(file.get());
		}
		return fileToInsert;
	}

	public User findOrCreateUser(Integer id) {
		User u = em.find(User.class, id);
		if (u == null) {
			return createUser(id);
		} else {
			return u;
		}
	}

	private User createUser(int id) {
		E621Response<FullUserApi> response = client.getUserById(id);
		if (response.isSuccess()) {
			FullUserApi user = response.unwrap();
			User dbUser = new User();
			dbUser.setId(user.getId());
			dbUser.setCreatedAt(new Timestamp(user.getCreatedAt().getTime()));
			dbUser.setName(user.getName());
			dbUser.setLevel(Level.from(user.getLevel()).get());
			dbUser.setIsBanned(user.getIsBanned());
			em.persist(dbUser);
			dbUser.setAvatar(findOrCreatePost(user.getAvatarId().orElse(null)));
			return dbUser;
		} else {
			return null;
		}
	}

	private List<Tag> findOrCreateTags(List<String> tags) {
		List<Tag> alreadyPersistedTags = getTagsByName(tags);

		for (Tag tag : alreadyPersistedTags) {
			if (tags.contains(tag.getText())) {
				tags.remove(tag.getText());
			}
		}

		// Seperate the list into many smaller lists
		int chunkSize = 50;
		List<List<String>> chunks = new ArrayList<>();
		for (int i = 0; i < tags.size(); i += chunkSize) {
			chunks.add(tags.subList(i, Math.min(i + chunkSize, tags.size())));
		}

		for (List<String> list : chunks) {
			List<TagApi> response = client.getTagsByName(list).unwrap();
			for (TagApi tag : response) {
				Tag dbTag = new Tag();
				dbTag.setId(tag.getId());
				dbTag.setTagType(TagType.from(tag.getCategory()).get());
				dbTag.setText(tag.getName());
				em.persist(dbTag);
				alreadyPersistedTags.add(dbTag);
			}
		}

		return alreadyPersistedTags;
	}

	private List<Tag> getTagsByName(List<String> tags) {
		CriteriaBuilder cb = em.getCriteriaBuilder();
		CriteriaQuery<Tag> cq = cb.createQuery(Tag.class);
		Root<Tag> root = cq.from(Tag.class);
		cq.where(root.get(Tag_.text).in(tags));
		TypedQuery<Tag> q = em.createQuery(cq);
		return q.getResultList();
	}
}
