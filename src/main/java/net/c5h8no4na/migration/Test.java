package net.c5h8no4na.migration;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.transaction.Transactional;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.c5h8no4na.e621.api.E621Client;
import net.c5h8no4na.e621.api.E621Response;
import net.c5h8no4na.e621.api.response.FullUserApi;
import net.c5h8no4na.e621.api.response.PostApi;
import net.c5h8no4na.e621.api.response.TagApi;
import net.c5h8no4na.e621.api.response.partial.File;
import net.c5h8no4na.e621.api.response.partial.Relationships;
import net.c5h8no4na.e621.api.response.partial.Score;
import net.c5h8no4na.e621.api.response.partial.Tags;
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

@Stateless
@Path("/")
public class Test {
	@PersistenceContext(unitName = "e621")
	private EntityManager em;

	private E621Client client = new E621Client("earlopain/test");

	@Transactional
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String test(@QueryParam("id") Integer id) {
		Gson gson = new GsonBuilder().serializeNulls().create();
		return gson.toJson(postToApiPost(findOrCreatePost(id)));
	}

	@GET
	@Path("/file")
	public Response getFile(@QueryParam("id") Integer id) {
		Post p = em.find(Post.class, id);
		if (p == null) {
			return Response.serverError().status(Status.NOT_FOUND).build();
		}
		Optional<PostFile> pf = p.getPostFile();
		if (pf.isEmpty()) {
			return Response.serverError().status(Status.NOT_FOUND).build();
		} else {
			return Response.ok(pf.get().getFile()).type(p.getExtension().toMediaType()).build();
		}
	}

	private Post findOrCreatePost(Integer id) {
		if (id == null) {
			return null;
		}
		Post p = em.find(Post.class, id);
		if (p == null) {
			E621Response<PostApi> response = client.getPost(id);
			if (response.getSuccess()) {
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

				if (post.getApproverId().isPresent()) {
					dbPost.setApprover(findOrCreateUser(post.getApproverId().get()));
				}
				dbPost.setUploader(findOrCreateUser(post.getUploaderId()));
				dbPost.setDescription(post.getDescription());
				dbPost.setDuration(post.getDuration().orElse(null));

				em.persist(dbPost);
				// File
				if (!post.getFlags().getDeleted()) {
					Optional<byte[]> file = client.getFile(dbPost.getMd5(), dbPost.getExtension().toString().toLowerCase());
					PostFile pf = new PostFile();
					pf.setPost(dbPost);
					pf.setFile(file.get());
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
				return null;
			}

		} else {
			return p;
		}
	}

	private User findOrCreateUser(Integer id) {
		User u = em.find(User.class, id);
		if (u == null) {
			E621Response<FullUserApi> response = client.getUserById(id);
			if (response.getSuccess()) {
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
		} else {
			return u;
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

		em.flush();

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

	private PostApi postToApiPost(Post post) {
		if (post == null) {
			return null;
		}
		PostApi p = new PostApi();
		p.setId(post.getId());
		p.setCreatedAt(post.getCreatedAt());
		p.setUpdatedAt(post.getUpdatedAt());

		File f = new File();
		f.setWidth(post.getWidth());
		f.setHeight(post.getHeight());
		f.setExt(post.getExtension().name().toLowerCase());
		f.setSize(post.getSize());
		f.setMd5(post.getMd5());
		p.setFile(f);

		Score s = new Score();
		s.setUp(post.getScoreUp());
		s.setDown(post.getScoreDown());
		s.setTotal(post.getScoreTotal());
		p.setScore(s);

		List<Tag> tags = post.getTags();

		Tags t = new Tags();
		t.setArtist(tags.stream().filter(tag -> tag.getTagType() == TagType.ARTIST).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setCharacter(
				tags.stream().filter(tag -> tag.getTagType() == TagType.CHARACTER).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setCopyright(
				tags.stream().filter(tag -> tag.getTagType() == TagType.COPYRIGHT).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setGeneral(
				tags.stream().filter(tag -> tag.getTagType() == TagType.GENERAL).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setInvalid(
				tags.stream().filter(tag -> tag.getTagType() == TagType.INVALID).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setLore(tags.stream().filter(tag -> tag.getTagType() == TagType.LORE).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setMeta(tags.stream().filter(tag -> tag.getTagType() == TagType.META).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setSpecies(
				tags.stream().filter(tag -> tag.getTagType() == TagType.SPECIES).map(tag -> tag.getText()).collect(Collectors.toList()));
		p.setTags(t);

		p.setRating(Character.toString(post.getRating().name().toLowerCase().charAt(0)));
		p.setFavCount(post.getFavCount());

		Relationships r = new Relationships();
		r.setChildren(post.getChildren().stream().map(c -> c.getId()).collect(Collectors.toList()));
		r.setParentId(post.getParent().isPresent() ? post.getParent().get().getId() : null);
		p.setRelationships(r);

		p.setApproverId(post.getApprover().isPresent() ? post.getApprover().get().getId() : null);
		p.setUploaderId(post.getUploader().getId());
		p.setDescription(post.getDescription());
		p.setDuration(post.getDuration().orElse(null));
		p.setSources(post.getSources().stream().map(source -> source.getSource()).collect(Collectors.toList()));

		return p;
	}
}