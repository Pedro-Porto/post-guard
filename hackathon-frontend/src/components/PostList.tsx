// src/components/PostList.tsx
import type { PostType } from '../types';
import { Post } from './Post';
import { PostDetails } from './PostDetails';
import type { PostDetailsType } from '../types';

type PostListProps = {
  posts: PostType[];
  selectedPostId: number | null;
  postDetails: PostDetailsType | null;
  isLoading: boolean;
  onPostSelect: (id: number) => void;
};

export function PostList({ posts, selectedPostId, postDetails, isLoading, onPostSelect }: PostListProps) {
  return (
    <div className="mt-6">
      {posts.map((post) => (
        <div key={post.id}>
          <Post
            post={post}
            onPostSelect={onPostSelect}
            isSelected={selectedPostId === post.id}
          />
          {selectedPostId === post.id && (
            <div>
              {isLoading && <p className="text-center p-4 text-gray-500">Analisando...</p>}
              {!isLoading && postDetails && <PostDetails details={postDetails} />}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}